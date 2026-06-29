//! The shared backend-conformance suite (the 16 port-level no-stub scenarios) run against the sqlite
//! backend. Each scenario gets a fresh `:memory:` database.

use pqueue_sqlite::SqliteBackend;

pqueue_conformance::conformance_suite!(|| SqliteBackend::in_memory().expect("open :memory:"));

/// B1a (ADR-009 / TD-003): a claim stamped with the owner's *cached* acquire-time epoch is fenced at the
/// durable append once a newer epoch is acquired (the owner was superseded), and leases nothing; the
/// current-epoch owner claims normally. Mirrors the memory white-box test against the sqlite log path.
#[tokio::test]
async fn claim_fences_superseded_owner_epoch() {
    use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
    use pqueue_engine::{
        ClaimPort, ClaimRequest, ControlPlaneStore, EngineError, ProjectionRead, PushCommand,
        QueueCommand,
    };

    let b = SqliteBackend::in_memory().expect("open :memory:");
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(e1 >= 1, "acquire advances the durable epoch");

    let stale = ClaimRequest {
        expected_epoch: Some(0),
        ..claim_req(10, 500, 100)
    };
    assert!(
        matches!(b.claim(stale).await, Err(EngineError::EpochFenced)),
        "a superseded owner's claim must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        0,
        "a fenced claim must lease nothing (durable append rejected before apply)"
    );

    let ok = ClaimRequest {
        expected_epoch: Some(e1),
        ..claim_req(10, 500, 100)
    };
    let claimed = b.claim(ok).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "current-epoch owner claims the item"
    );
}

/// B1b (ADR-009 / TD-003): the cached-epoch fence applies to `PushPort::push` on the sqlite log path too.
#[tokio::test]
async fn push_fences_superseded_owner_epoch() {
    use pqueue_conformance::{qdef, qkey, shard, ts};
    use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushPort, PushSpec};

    let b = SqliteBackend::in_memory().expect("open :memory:");
    b.create_queue(qdef()).await.unwrap();
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(e1 >= 1);

    assert!(
        matches!(
            b.push(&shard(), vec![PushSpec::default()], ts(0), Some(0))
                .await,
            Err(EngineError::EpochFenced)
        ),
        "a superseded owner's push must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        0,
        "a fenced push must append nothing"
    );

    let ids = b
        .push(&shard(), vec![PushSpec::default()], ts(1), Some(e1))
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
}
