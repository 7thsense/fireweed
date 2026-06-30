//! The shared backend-conformance suite (the 16 port-level no-stub scenarios) run against the sqlite
//! backend. Each scenario gets a fresh `:memory:` database.

use pqueue_sqlite::SqliteBackend;

pqueue_conformance::conformance_suite!(|| SqliteBackend::in_memory().expect("open :memory:"));

/// ADR-012 Phase 1: the SAME shared conformance suite against the COMPOSED sqlite backend
/// (`ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>`). Passing identically to the
/// monolith above proves the orthogonal composition is faithful before the monolith is removed (Phase 2).
mod composed {
    use pqueue_sqlite::composed_sqlite_backend_in_memory;
    pqueue_conformance::conformance_suite!(
        || composed_sqlite_backend_in_memory().expect("compose :memory:")
    );
}

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

fn typed_qdef() -> pqueue_core::QueueDefinition {
    use pqueue_conformance::qdef;
    use pqueue_core::EntitySchemaDocument;
    use serde_json::json;

    let mut def = qdef();
    def.entity_schema = Some(
        serde_json::from_value::<EntitySchemaDocument>(json!({
            "entity_schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"}
                }
            }
        }))
        .unwrap(),
    );
    def
}

fn typed_valid_item() -> pqueue_engine::PushSpec {
    pqueue_engine::PushSpec {
        entity: Some(serde_json::json!({"name": "ok"})),
        ..Default::default()
    }
}

fn typed_invalid_item() -> pqueue_engine::PushSpec {
    pqueue_engine::PushSpec {
        entity: Some(serde_json::json!({"count": 1})),
        ..Default::default()
    }
}

async fn schema_validation_backend<B>(backend: &B)
where
    B: pqueue_engine::ControlPlaneStore
        + pqueue_engine::PushPort
        + pqueue_engine::ProjectionRead,
{
    use pqueue_core::RequestId;
    use pqueue_engine::EngineError;

    let shard = pqueue_conformance::shard();
    backend.create_queue(typed_qdef()).await.unwrap();

    let err = backend
        .push(&shard, vec![typed_invalid_item()], pqueue_conformance::ts(0), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 0);

    let rid = RequestId::new("req-1").unwrap();
    let err = backend
        .push_with_request_id(
            &shard,
            rid.clone(),
            vec![typed_invalid_item()],
            pqueue_conformance::ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 0);

    let first = backend
        .push_with_request_id(
            &shard,
            rid.clone(),
            vec![typed_valid_item()],
            pqueue_conformance::ts(2),
            None,
        )
        .await
        .unwrap();
    let replay = backend
        .push_with_request_id(
            &shard,
            rid,
            vec![typed_valid_item()],
            pqueue_conformance::ts(3),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
}

#[tokio::test]
async fn schema_validation_rejects_before_append_and_idempotency_on_sqlite_log() {
    let backend = SqliteBackend::in_memory().expect("open :memory:");
    schema_validation_backend(&backend).await;
}
