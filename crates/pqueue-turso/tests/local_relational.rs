use std::time::Duration;

use pqueue_relational::OWNED_PROJECTION_TABLES;
use pqueue_turso::{
    JournalMode, RelationalStatement, TursoConfig, TursoRelational, TursoRelationalError,
};
use tempfile::tempdir;
use turso::Value;

#[tokio::test]
async fn configures_and_verifies_the_exact_shared_schema() {
    let store = TursoRelational::in_memory().await.expect("open Turso");
    let settings = store.connection_settings().await.expect("settings");
    assert_eq!(settings.journal_mode, "wal");
    assert_eq!(settings.synchronous, 1);
    assert_eq!(settings.busy_timeout_ms, 5_000);

    let report = store.schema_report().await.expect("schema report");
    for table in OWNED_PROJECTION_TABLES {
        assert!(report.tables.iter().any(|actual| actual == table));
    }
    for index in [
        "pqueue_items_active_key",
        "pqueue_items_group_due_idx",
        "pqueue_item_index_key_idx",
    ] {
        assert!(report.indexes.iter().any(|actual| actual == index));
    }
}

#[tokio::test]
async fn migration_is_idempotent_and_state_survives_reopen() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("projection.db");
    let config = TursoConfig::local(&path).with_busy_timeout(Duration::from_millis(2_500));
    let store = TursoRelational::open(config.clone()).await.expect("open");
    store.migrate().await.expect("second migration");

    store
        .execute_immediate(&[
            RelationalStatement::new(
                "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)",
                vec!["t".into(), "q".into(), "{}".into()],
            ),
            RelationalStatement::new(
                "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq,assignment_epoch) \
                 VALUES(?1,?2,?3,?4,?5)",
                vec!["t".into(), "q".into(), 7_i64.into(), 3_i64.into(), 2_i64.into()],
            ),
        ])
        .await
        .expect("atomic seed");
    drop(store);

    let reopened = TursoRelational::open(config).await.expect("reopen");
    let settings = reopened.connection_settings().await.expect("settings");
    assert_eq!(settings.journal_mode, "wal");
    assert_eq!(settings.busy_timeout_ms, 2_500);
    let rows = reopened
        .query(
            "SELECT next_seq,next_item_seq,assignment_epoch FROM relational_cursor \
             WHERE tenant=?1 AND queue=?2",
            vec!["t".into(), "q".into()],
        )
        .await
        .expect("cursor");
    assert_eq!(
        rows[0].values,
        vec![Value::Integer(7), Value::Integer(3), Value::Integer(2)]
    );
}

#[tokio::test]
async fn immediate_batch_rolls_back_every_statement_on_error() {
    let store = TursoRelational::in_memory().await.expect("open");
    let result = store
        .execute_immediate(&[
            RelationalStatement::new(
                "INSERT INTO pqueue_side_records(tenant_id,queue_id,key,payload) \
                 VALUES(?1,?2,?3,?4)",
                vec!["t".into(), "q".into(), vec![1_u8].into(), vec![2_u8].into()],
            ),
            RelationalStatement::new(
                "INSERT INTO pqueue_side_records(tenant_id,queue_id,key,payload) \
                 VALUES(?1,?2,?3,?4)",
                vec!["t".into(), "q".into(), vec![1_u8].into(), vec![3_u8].into()],
            ),
        ])
        .await;
    assert!(matches!(result, Err(TursoRelationalError::Database(_))));

    let rows = store
        .query("SELECT COUNT(*) FROM pqueue_side_records", vec![])
        .await
        .expect("count");
    assert_eq!(rows[0].values, vec![Value::Integer(0)]);
}

#[tokio::test]
async fn rejects_invalid_config_before_opening() {
    let result = TursoRelational::open(
        TursoConfig::in_memory()
            .with_busy_timeout(Duration::ZERO)
            .with_journal_mode(JournalMode::Mvcc),
    )
    .await;
    assert!(matches!(
        result,
        Err(TursoRelationalError::Configuration(_))
    ));
}
