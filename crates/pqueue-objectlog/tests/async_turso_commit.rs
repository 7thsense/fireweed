use pqueue_conformance::{envelope, item, qdef};
use pqueue_core::{ItemId, ItemState};
use pqueue_engine::{
    AsyncCommitStrategy, AsyncLogStore, AsyncProjectionStore, DurabilityClass, PushCommand,
    QueueCommand, QueueKey, RawCommitRequest, SeparateReplayCommit, SeparateReplayCommitter,
};
use pqueue_objectlog::{AsyncObjectLog, ObjectLogProjectionCommitter};
use pqueue_turso::{TursoConfig, TursoRelational};

#[tokio::test(flavor = "current_thread")]
async fn object_log_commit_crosses_the_real_turso_response_barrier() {
    let root = std::env::temp_dir().join(format!("pqueue-objectlog-turso-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let log = AsyncObjectLog::open(&root).await.unwrap();
    log.ensure_shard(shard.clone()).await.unwrap();
    let projection = Arc::new(TursoRelational::in_memory().await.unwrap());
    projection.ensure_shard(definition.clone()).await.unwrap();
    let committer = ObjectLogProjectionCommitter::open_shared(
        log.clone(),
        Arc::clone(&projection),
        vec![definition.clone()],
        16,
        8,
    )
    .await
    .unwrap();
    let strategy =
        SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer).unwrap();
    let id = ItemId::new("1").unwrap();
    let outcome = strategy
        .commit(RawCommitRequest::new(
            shard.clone(),
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("1", "one", 1)],
                }),
                vec![id],
            )],
            0,
        ))
        .await
        .unwrap();

    assert!(outcome.projection_applied());
    assert_eq!(
        projection.item_state(shard.clone(), id).await.unwrap(),
        Some(ItemState::Pending)
    );
    assert_eq!(
        projection.recovery_high_water(shard).await.unwrap(),
        Some(outcome.positions()[0].clone())
    );
    log.close_and_drain().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "current_thread")]
async fn reopen_automatically_repairs_append_only_tail_before_later_commit() {
    let base = std::env::temp_dir().join(format!(
        "pqueue-objectlog-turso-reopen-{}",
        std::process::id()
    ));
    let log_path = base.join("log");
    let projection_path = base.join("projection.db");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let first_id = ItemId::new("1").unwrap();

    {
        let log = AsyncObjectLog::open(&log_path).await.unwrap();
        log.ensure_shard(shard.clone()).await.unwrap();
        let projection = Arc::new(
            TursoRelational::open(TursoConfig::local(&projection_path))
                .await
                .unwrap(),
        );
        projection.ensure_shard(definition.clone()).await.unwrap();
        let committer = ObjectLogProjectionCommitter::open_shared(
            log.clone(),
            Arc::clone(&projection),
            vec![definition.clone()],
            1,
            8,
        )
        .await
        .unwrap();
        let appended = committer
            .commit_replayable(
                RawCommitRequest::new(
                    shard.clone(),
                    vec![envelope(
                        QueueCommand::Push(PushCommand {
                            items: vec![item("1", "one", 1)],
                        }),
                        vec![first_id],
                    )],
                    0,
                )
                .with_fault(pqueue_engine::RawCommitFault::AfterAppendBeforeApply),
            )
            .await
            .unwrap();
        assert!(!appended.projection_applied());
        assert_eq!(
            projection
                .item_state(shard.clone(), first_id)
                .await
                .unwrap(),
            None
        );
        drop(committer);
        drop(projection);
        log.close_and_drain().await.unwrap();
    }

    // The projection is disposable: remove the entire local Turso cache before reopening.
    for path in [
        projection_path.clone(),
        projection_path.with_extension("db-wal"),
        projection_path.with_extension("db-shm"),
    ] {
        let _ = std::fs::remove_file(path);
    }

    {
        let log = AsyncObjectLog::open(&log_path).await.unwrap();
        let projection = Arc::new(
            TursoRelational::open(TursoConfig::local(&projection_path))
                .await
                .unwrap(),
        );
        let committer = ObjectLogProjectionCommitter::open_shared(
            log.clone(),
            Arc::clone(&projection),
            vec![definition.clone()],
            1,
            8,
        )
        .await
        .unwrap();
        assert_eq!(
            projection
                .item_state(shard.clone(), first_id)
                .await
                .unwrap(),
            Some(ItemState::Pending),
            "open must repair the durable append-only tail"
        );
        let second_id = ItemId::new("2").unwrap();
        let later = committer
            .commit_replayable(RawCommitRequest::new(
                shard.clone(),
                vec![envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("2", "two", 1)],
                    }),
                    vec![second_id],
                )],
                0,
            ))
            .await
            .unwrap();
        assert_eq!(later.positions().len(), 1);
        assert_eq!(
            projection
                .item_state(shard.clone(), first_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            projection
                .item_state(shard.clone(), second_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            log.read_from(shard, None, 10).await.unwrap().entries.len(),
            2,
            "repair must not append or duplicate commands"
        );
        log.close_and_drain().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(base);
}
use std::sync::Arc;
