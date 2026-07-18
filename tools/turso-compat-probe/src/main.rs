use std::sync::Arc;
use std::time::Duration;

use tempfile::tempdir;
use tokio::sync::{Barrier, oneshot};
use turso::{Builder, Database, Error as TursoError, transaction::TransactionBehavior};

const HELPERS_RS: &str = include_str!("../../../crates/pqueue-sqlite/src/relational/helpers.rs");
const WRITER_COUNT: usize = 16;

#[derive(Debug, PartialEq, Eq)]
struct LeaseState {
    lifecycle_state: String,
    lease_token_hash: Vec<u8>,
    lease_expires_at: i64,
    worker_id: String,
    retry_count: i64,
    item_version: i64,
    last_command_sequence: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct ProjectionState {
    item_count: i64,
    cursor: i64,
    eligible: Vec<String>,
    index_items: Vec<String>,
    lease: LeaseState,
}

fn relational_schema() -> &'static str {
    let marker = "pub(crate) const RELATIONAL_SCHEMA: &str = r#\"";
    let start = HELPERS_RS
        .find(marker)
        .expect("RELATIONAL_SCHEMA declaration")
        + marker.len();
    let tail = &HELPERS_RS[start..];
    let end = tail.find("\"#;").expect("RELATIONAL_SCHEMA terminator");
    &tail[..end]
}

fn item_insert(id: &str, key: &str, priority_hex: &str, created_seq: u64) -> String {
    format!(
        "INSERT INTO pqueue_items(tenant_id,queue_id,item_id,client_item_key,lifecycle_state,\
         priority,priority_sort,not_before,eligible_since,group_key,cohort_size,recurrence_until,\
         payload,fields,metadata,entity_document,retry_count,item_version,lease_token_hash,\
         lease_expires_at,worker_id,last_command_sequence,created_at,updated_at,terminal_at,\
         terminal_command_epoch,fenced,superseded,max_attempts,created_seq) VALUES(\
         't','q','{id}','{key}','Pending',NULL,X'{priority_hex}',NULL,1,NULL,NULL,NULL,\
         X'CAFE','{{}}','{{}}',NULL,0,1,NULL,NULL,NULL,{created_seq},{created_seq},{created_seq},\
         NULL,NULL,0,0,3,{created_seq})"
    )
}

async fn scalar_i64(conn: &turso::Connection, sql: &str) -> Result<i64, TursoError> {
    let mut rows = conn.query(sql, ()).await?;
    let row = rows.next().await?.ok_or(TursoError::QueryReturnedNoRows)?;
    row.get(0)
}

async fn text_rows(conn: &turso::Connection, sql: &str) -> Result<Vec<String>, TursoError> {
    let mut rows = conn.query(sql, ()).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(row.get(0)?);
    }
    Ok(out)
}

async fn text_scalar(conn: &turso::Connection, sql: &str) -> Result<String, TursoError> {
    let mut rows = conn.query(sql, ()).await?;
    let row = rows.next().await?.ok_or(TursoError::QueryReturnedNoRows)?;
    row.get(0)
}

async fn turso_projection_state(
    conn: &turso::Connection,
    eligible_sql: &str,
) -> Result<ProjectionState, TursoError> {
    let mut lease_rows = conn
        .query(
            "SELECT lifecycle_state,lease_token_hash,lease_expires_at,worker_id,retry_count,\
             item_version,last_command_sequence FROM pqueue_items WHERE item_id='item-a'",
            (),
        )
        .await?;
    let lease = lease_rows
        .next()
        .await?
        .ok_or(TursoError::QueryReturnedNoRows)?;
    let lease = LeaseState {
        lifecycle_state: lease.get(0)?,
        lease_token_hash: lease.get(1)?,
        lease_expires_at: lease.get(2)?,
        worker_id: lease.get(3)?,
        retry_count: lease.get(4)?,
        item_version: lease.get(5)?,
        last_command_sequence: lease.get(6)?,
    };
    Ok(ProjectionState {
        item_count: scalar_i64(conn, "SELECT COUNT(*) FROM pqueue_items").await?,
        cursor: scalar_i64(
            conn,
            "SELECT next_seq FROM relational_cursor WHERE tenant='t' AND queue='q'",
        )
        .await?,
        eligible: text_rows(conn, eligible_sql).await?,
        index_items: text_rows(
            conn,
            "SELECT item_id FROM pqueue_item_index WHERE tenant_id='t' AND queue_id='q' \
             AND index_name='probe' AND index_key>=X'10' ORDER BY index_key,item_id",
        )
        .await?,
        lease,
    })
}

fn rusqlite_text_rows(conn: &rusqlite::Connection, sql: &str) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect()
}

fn rusqlite_projection_state(
    conn: &rusqlite::Connection,
    eligible_sql: &str,
) -> rusqlite::Result<ProjectionState> {
    let lease = conn.query_row(
        "SELECT lifecycle_state,lease_token_hash,lease_expires_at,worker_id,retry_count,\
         item_version,last_command_sequence FROM pqueue_items WHERE item_id='item-a'",
        [],
        |row| {
            Ok(LeaseState {
                lifecycle_state: row.get(0)?,
                lease_token_hash: row.get(1)?,
                lease_expires_at: row.get(2)?,
                worker_id: row.get(3)?,
                retry_count: row.get(4)?,
                item_version: row.get(5)?,
                last_command_sequence: row.get(6)?,
            })
        },
    )?;
    Ok(ProjectionState {
        item_count: conn.query_row("SELECT COUNT(*) FROM pqueue_items", [], |row| row.get(0))?,
        cursor: conn.query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant='t' AND queue='q'",
            [],
            |row| row.get(0),
        )?,
        eligible: rusqlite_text_rows(conn, eligible_sql)?,
        index_items: rusqlite_text_rows(
            conn,
            "SELECT item_id FROM pqueue_item_index WHERE tenant_id='t' AND queue_id='q' \
             AND index_name='probe' AND index_key>=X'10' ORDER BY index_key,item_id",
        )?,
        lease,
    })
}

fn retryable(error: &TursoError) -> bool {
    matches!(error, TursoError::Busy(_) | TursoError::BusySnapshot(_))
}

async fn disjoint_writer(db: Database, writer: usize, gate: Arc<Barrier>) -> Result<usize, String> {
    let mut conn = db.connect().map_err(|e| format!("connect: {e:?}"))?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(|e| format!("busy timeout: {e:?}"))?;
    gate.wait().await;
    for attempt in 1..=128 {
        let tx = match conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
        {
            Ok(tx) => tx,
            Err(error) if retryable(&error) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => return Err(format!("begin writer {writer}: {error:?}")),
        };
        let sql = format!(
            "INSERT INTO probe_disjoint_writers(writer_id,value) VALUES({writer},'writer-{writer}')"
        );
        match tx.execute(sql, ()).await {
            Ok(_) => match tx.commit().await {
                Ok(()) => return Ok(attempt),
                Err(error) if retryable(&error) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(format!("commit writer {writer}: {error:?}")),
            },
            Err(error) if retryable(&error) => {
                let _ = tx.rollback().await;
                tokio::task::yield_now().await;
            }
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(format!("insert writer {writer}: {error:?}"));
            }
        }
    }
    Err(format!("writer {writer} exhausted retry budget"))
}

async fn run_turso(
    path: &str,
) -> Result<(ProjectionState, usize, String), Box<dyn std::error::Error>> {
    let db = Builder::new_local(path).build().await?;
    let mut conn = db.connect()?;
    conn.busy_timeout(Duration::from_secs(5))?;

    let exact_pragma_error = conn
        .execute_batch(
            "PRAGMA journal_mode=WAL;PRAGMA synchronous=NORMAL;PRAGMA busy_timeout=5000;",
        )
        .await
        .expect_err("row-producing journal_mode must expose the observed API mismatch");
    assert!(
        matches!(&exact_pragma_error, TursoError::Misuse(message) if message == "unexpected row during execution")
    );
    let journal_mode_after_error = text_scalar(&conn, "PRAGMA journal_mode").await?;
    assert_eq!(
        journal_mode_after_error, "wal",
        "the failed batch must expose its partial journal-mode side effect"
    );
    let journal_rows = conn.pragma_update("journal_mode", "WAL").await?;
    assert_eq!(journal_rows.len(), 1);
    conn.pragma_update("synchronous", "NORMAL").await?;
    conn.busy_timeout(Duration::from_secs(5))?;
    assert_eq!(text_scalar(&conn, "PRAGMA journal_mode").await?, "wal");
    assert_eq!(scalar_i64(&conn, "PRAGMA synchronous").await?, 1);
    assert_eq!(scalar_i64(&conn, "PRAGMA busy_timeout").await?, 5000);
    conn.execute_batch(relational_schema()).await?;

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    tx.execute(
        "INSERT INTO queues(tenant,queue,definition,paused) VALUES('t','q','{}',0)",
        (),
    )
    .await?;
    tx.execute(
        "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq,assignment_epoch) \
         VALUES('t','q',0,1,0)",
        (),
    )
    .await?;
    tx.execute(item_insert("item-a", "key-a", "02", 1), ())
        .await?;
    tx.execute(item_insert("item-b", "key-b", "01", 2), ())
        .await?;
    tx.execute(item_insert("item-c", "key-c", "03", 3), ())
        .await?;
    tx.execute(
        "INSERT INTO pqueue_item_index(tenant_id,queue_id,index_name,index_key,item_id) \
         VALUES('t','q','probe',X'10','item-a')",
        (),
    )
    .await?;
    tx.execute(
        "INSERT INTO pqueue_group_summary(tenant_id,queue_id,group_key,oldest_eligible_at,\
         rep_progress_guard_sort,rep_priority_sort,rep_created_at,rep_item_id,eligible_item_count,\
         at_risk_count,updated_at) VALUES('t','q','g',1,NULL,X'01',2,'item-b',2,0,4) \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
         eligible_item_count=excluded.eligible_item_count,updated_at=excluded.updated_at",
        (),
    )
    .await?;
    tx.execute(
        "UPDATE pqueue_items SET lifecycle_state='Leased',lease_token_hash=X'A1B2',\
         lease_expires_at=1000,worker_id='worker-a',retry_count=retry_count+1,\
         item_version=item_version+1,updated_at=4,last_command_sequence=4 \
         WHERE tenant_id='t' AND queue_id='q' AND item_id='item-a'",
        (),
    )
    .await?;
    tx.execute(
        "UPDATE relational_cursor SET next_seq=4,next_item_seq=4 WHERE tenant='t' AND queue='q'",
        (),
    )
    .await?;
    tx.commit().await?;

    let eligible_sql = "SELECT item_id FROM pqueue_items WHERE tenant_id='t' AND queue_id='q' \
        AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
        AND (not_before IS NULL OR not_before<=10) AND eligible_since IS NOT NULL \
        AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
          ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
          WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
          AND ig.item_id=pqueue_items.item_id) ORDER BY priority_sort,created_seq LIMIT 10";
    let eligible = text_rows(&conn, eligible_sql).await?;
    assert_eq!(eligible, ["item-b", "item-c"]);
    assert_eq!(
        text_rows(
            &conn,
            "WITH candidates AS (SELECT item_id FROM pqueue_items WHERE tenant_id='t' \
             AND queue_id='q' ORDER BY priority_sort,created_seq LIMIT 1) SELECT item_id FROM candidates"
        )
        .await?,
        ["item-b"]
    );
    let committed_state = turso_projection_state(&conn, eligible_sql).await?;
    assert_eq!(committed_state.item_count, 3);
    assert_eq!(committed_state.cursor, 4);
    assert_eq!(committed_state.eligible, ["item-b", "item-c"]);
    assert_eq!(committed_state.index_items, ["item-a"]);
    assert_eq!(
        committed_state.lease,
        LeaseState {
            lifecycle_state: "Leased".into(),
            lease_token_hash: vec![0xA1, 0xB2],
            lease_expires_at: 1000,
            worker_id: "worker-a".into(),
            retry_count: 1,
            item_version: 2,
            last_command_sequence: 4,
        }
    );

    let rollback = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    rollback
        .execute(item_insert("rolled-back", "rolled-back", "00", 3), ())
        .await?;
    rollback
        .execute(
            "UPDATE relational_cursor SET next_seq=5 WHERE tenant='t' AND queue='q'",
            (),
        )
        .await?;
    rollback.rollback().await?;
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM pqueue_items WHERE item_id='rolled-back'"
        )
        .await?,
        0
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT next_seq FROM relational_cursor WHERE tenant='t' AND queue='q'"
        )
        .await?,
        4
    );

    let mut checkpoint = conn.query("PRAGMA wal_checkpoint(PASSIVE)", ()).await?;
    let checkpoint = checkpoint
        .next()
        .await?
        .ok_or("wal_checkpoint returned no row")?;
    let checkpoint_shape = format!(
        "({},{},{})",
        checkpoint.get::<i64>(0)?,
        checkpoint.get::<i64>(1)?,
        checkpoint.get::<i64>(2)?
    );
    drop(conn);
    drop(db);

    let reopened = Builder::new_local(path).build().await?;
    let reopened_conn = reopened.connect()?;
    let reopened_state = turso_projection_state(&reopened_conn, eligible_sql).await?;
    assert_eq!(
        reopened_state, committed_state,
        "Turso item/index/cursor/eligible/lease state must agree after reopen"
    );
    reopened_conn
        .execute_batch(
            "CREATE TABLE probe_disjoint_writers(writer_id INTEGER PRIMARY KEY,value TEXT NOT NULL);",
        )
        .await?;

    let gate = Arc::new(Barrier::new(WRITER_COUNT + 1));
    let mut handles = Vec::with_capacity(WRITER_COUNT);
    for writer in 0..WRITER_COUNT {
        handles.push(tokio::spawn(disjoint_writer(
            reopened.clone(),
            writer,
            gate.clone(),
        )));
    }
    gate.wait().await;
    let mut attempts = Vec::with_capacity(WRITER_COUNT);
    for handle in handles {
        attempts.push(handle.await.map_err(|e| format!("writer task: {e}"))??);
    }
    assert_eq!(attempts.len(), WRITER_COUNT, "all 16 tasks must return");
    assert_eq!(
        scalar_i64(
            &reopened_conn,
            "SELECT COUNT(*) FROM probe_disjoint_writers"
        )
        .await?,
        WRITER_COUNT as i64,
        "all 16 disjoint rows must commit"
    );
    let distinct = scalar_i64(
        &reopened_conn,
        "SELECT COUNT(DISTINCT writer_id) FROM probe_disjoint_writers",
    )
    .await?;
    assert_eq!(distinct, WRITER_COUNT as i64, "writer ids must be distinct");

    let mut winner_conn = reopened.connect()?;
    let winner_tx = winner_conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    winner_tx
        .execute(item_insert("same-key-a", "same-key", "03", 100), ())
        .await?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let loser_db = reopened.clone();
    let loser = tokio::spawn(async move {
        let mut conn = loser_db.connect().map_err(|e| format!("connect: {e:?}"))?;
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|e| format!("timeout: {e:?}"))?;
        ready_tx.send(()).map_err(|_| "ready send".to_string())?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(|e| format!("begin: {e:?}"))?;
        match tx
            .execute(item_insert("same-key-b", "same-key", "04", 101), ())
            .await
        {
            Err(TursoError::Constraint(message)) => {
                let _ = tx.rollback().await;
                Ok::<String, String>(message)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(format!("unexpected loser error: {error:?}"))
            }
            Ok(_) => {
                let _ = tx.rollback().await;
                Err("same-key loser unexpectedly inserted".to_string())
            }
        }
    });
    ready_rx.await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    winner_tx.commit().await?;
    let conflict = loser.await.map_err(|e| format!("same-key task: {e}"))??;
    assert_eq!(
        text_rows(
            &reopened_conn,
            "SELECT item_id FROM pqueue_items WHERE tenant_id='t' AND queue_id='q' \
             AND client_item_key='same-key' AND superseded=0"
        )
        .await?,
        ["same-key-a"]
    );

    println!("turso.schema=pass");
    println!("turso.exact_pragma_batch=unsupported:{exact_pragma_error:?}");
    println!("turso.journal_mode_after_batch_error={journal_mode_after_error}");
    println!("turso.config_readback=journal_mode:wal synchronous:1 busy_timeout:5000");
    println!("turso.pragma_update=pass");
    println!("turso.batch_lifecycle_index_cursor_eligible_rollback_reopen=pass");
    println!("turso.wal_checkpoint_shape={checkpoint_shape}");
    println!(
        "turso.disjoint_writers=pass writers={} attempts={attempts:?}",
        attempts.len()
    );
    println!("turso.same_key=same-key-a-wins same-key-b-constraint:{conflict}");
    Ok((reopened_state, attempts.len(), conflict))
}

fn run_rusqlite(path: &str) -> Result<ProjectionState, Box<dyn std::error::Error>> {
    let mut conn = rusqlite::Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;PRAGMA synchronous=NORMAL;PRAGMA busy_timeout=5000;",
    )?;
    conn.execute_batch(relational_schema())?;
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO queues(tenant,queue,definition,paused) VALUES('t','q','{}',0)",
        [],
    )?;
    tx.execute(
        "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq,assignment_epoch) \
         VALUES('t','q',0,1,0)",
        [],
    )?;
    tx.execute(&item_insert("item-a", "key-a", "02", 1), [])?;
    tx.execute(&item_insert("item-b", "key-b", "01", 2), [])?;
    tx.execute(&item_insert("item-c", "key-c", "03", 3), [])?;
    tx.execute(
        "INSERT INTO pqueue_item_index(tenant_id,queue_id,index_name,index_key,item_id) \
         VALUES('t','q','probe',X'10','item-a')",
        [],
    )?;
    tx.execute(
        "UPDATE pqueue_items SET lifecycle_state='Leased',lease_token_hash=X'A1B2',\
         lease_expires_at=1000,worker_id='worker-a',retry_count=retry_count+1,\
         item_version=item_version+1,updated_at=4,last_command_sequence=4 \
         WHERE tenant_id='t' AND queue_id='q' AND item_id='item-a'",
        [],
    )?;
    tx.execute(
        "UPDATE relational_cursor SET next_seq=4,next_item_seq=4 WHERE tenant='t' AND queue='q'",
        [],
    )?;
    tx.commit()?;
    let eligible_sql = "SELECT item_id FROM pqueue_items WHERE tenant_id='t' AND queue_id='q' \
        AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
        AND (not_before IS NULL OR not_before<=10) AND eligible_since IS NOT NULL \
        ORDER BY priority_sort,created_seq LIMIT 10";
    let committed_state = rusqlite_projection_state(&conn, eligible_sql)?;
    assert_eq!(committed_state.item_count, 3);
    assert_eq!(committed_state.cursor, 4);
    assert_eq!(committed_state.eligible, ["item-b", "item-c"]);
    assert_eq!(committed_state.index_items, ["item-a"]);
    assert_eq!(
        committed_state.lease,
        LeaseState {
            lifecycle_state: "Leased".into(),
            lease_token_hash: vec![0xA1, 0xB2],
            lease_expires_at: 1000,
            worker_id: "worker-a".into(),
            retry_count: 1,
            item_version: 2,
            last_command_sequence: 4,
        }
    );
    let rollback = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    rollback.execute(&item_insert("rolled-back", "rolled-back", "00", 3), [])?;
    rollback.execute(
        "UPDATE relational_cursor SET next_seq=5 WHERE tenant='t' AND queue='q'",
        [],
    )?;
    rollback.rollback()?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM pqueue_items", [], |row| row.get(0))?;
    let cursor: i64 = conn.query_row(
        "SELECT next_seq FROM relational_cursor WHERE tenant='t' AND queue='q'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!((count, cursor), (3, 4));
    drop(conn);
    let reopened = rusqlite::Connection::open(path)?;
    let reopened_state = rusqlite_projection_state(&reopened, eligible_sql)?;
    assert_eq!(
        reopened_state, committed_state,
        "rusqlite item/index/cursor/eligible/lease state must agree after reopen"
    );
    reopened.execute(&item_insert("same-key-a", "same-key", "03", 100), [])?;
    let conflict = reopened
        .execute(&item_insert("same-key-b", "same-key", "04", 101), [])
        .expect_err("rusqlite same-key insert must conflict");
    assert!(matches!(conflict, rusqlite::Error::SqliteFailure(_, _)));
    println!("rusqlite.exact_pragma_batch=pass");
    println!("rusqlite.schema_batch_lifecycle_index_cursor_eligible_rollback_reopen=pass");
    let winner: String = reopened.query_row(
        "SELECT item_id FROM pqueue_items WHERE tenant_id='t' AND queue_id='q' \
         AND client_item_key='same-key' AND superseded=0",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(winner, "same-key-a");
    println!("rusqlite.same_key=same-key-a-wins same-key-b-constraint");
    Ok(reopened_state)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempdir()?;
    let turso_path = root.path().join("turso.db");
    let sqlite_path = root.path().join("rusqlite.db");
    let (turso_state, writers, _) = run_turso(turso_path.to_str().unwrap()).await?;
    let sqlite_state = run_rusqlite(sqlite_path.to_str().unwrap())?;
    assert_eq!(
        turso_state, sqlite_state,
        "cross-engine item/index/cursor/eligible/lease state must agree"
    );
    assert_eq!(writers, WRITER_COUNT);
    println!("comparison.reopened_projection_state=equal:{turso_state:?}");
    println!("decision=no-go-current-adapter");
    println!(
        "reason=The governing current-port stop rule rejects an adapter that would require changing \
         pqueue's synchronous ProjectionStore unit of work or adding a blocking database actor."
    );
    Ok(())
}
