//! The shared backend-conformance suite (the 16 port-level no-stub scenarios) run against the COMPOSED
//! sqlite backend (`ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>`). Each scenario
//! gets a fresh `:memory:` durable log.

use pqueue_sqlite::composed_sqlite_backend_in_memory;

pqueue_conformance::conformance_suite!(
    || composed_sqlite_backend_in_memory().expect("open :memory:")
);

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

    let b = composed_sqlite_backend_in_memory().expect("open :memory:");
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

    let b = composed_sqlite_backend_in_memory().expect("open :memory:");
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

async fn seeded_commit_transition_sqlite_backend() -> pqueue_sqlite::ComposedSqliteBackend {
    use bytes::Bytes;
    use pqueue_conformance::{claim_req, qdef, shard, ts};
    use pqueue_core::RequestId;
    use pqueue_engine::{
        ClaimPort, ClaimRef, CommitTransition, CommitTransitionEntry, CommitTransitionPort,
        ControlPlaneStore, FinalizeKind, InstanceFence, PushPort, PushSpec, SideRecord,
    };

    let b = composed_sqlite_backend_in_memory().expect("open :memory:");
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let claimed = b.claim(claim_req(1, 60, 0)).await.unwrap();
    let c = &claimed.items[0];
    let claim_ref = ClaimRef {
        item_id: c.item_id,
        lease_token: c.lease_token.clone().unwrap(),
        lease_expires_at: c.lease_expires_at,
        item_version: c.item_version,
    };
    let rid = RequestId::new("txn-commit-transition-1").unwrap();
    b.commit_transition(
        &shard(),
        CommitTransition {
            request_id: Some(rid),
            entries: vec![CommitTransitionEntry {
                claim_ref,
                finalize: FinalizeKind::Complete,
                side_records: vec![SideRecord {
                    key: b"state/run-1".to_vec(),
                    payload: Bytes::from_static(b"audit-bytes"),
                }],
                lifecycle_items: vec![PushSpec::default()],
                instance_fence: Some(InstanceFence {
                    instance_key: b"wf-1".to_vec(),
                    expected: 0,
                    next: 1,
                }),
            }],
        },
        ts(1),
        None,
    )
    .await
    .unwrap();
    b
}

/// Shared commit-transition positive scenario against the durable sqlite log-replay composition root.
#[tokio::test]
async fn commit_transition_shared_scenario_runs_against_sqlite_log_replay() {
    use pqueue_conformance::scenarios::commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let make_calls = std::sync::Arc::clone(&calls);
    let make = move || match make_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
        0 => composed_sqlite_backend_in_memory().expect("open :memory:"),
        _ => std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(seeded_commit_transition_sqlite_backend())
        })
        .join()
        .unwrap(),
    };

    commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(make)
        .await;
}
