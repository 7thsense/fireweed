//! Env-gated hot-projection capability smoke test for the postgres adapter.
//!
//! Without `PQUEUE_PG_TEST_URL` this prints a loud skip and returns green, so local CI does not depend
//! on a live database. When configured, it proves the adapter advertises hot-projection support
//! explicitly rather than pretending the capability exists.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue::RangeScanRequest;
use pqueue_conformance::qdef;
use pqueue_conformance::qkey;
use pqueue_core::{ItemId, LeaseToken, RequestId};
use pqueue_engine::{
    Backend, ClaimRef, CommitCapabilities, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, EngineError, FinalizeKind, HotProjectionQueryPort,
    RecoveryReadPort, SideRecord,
};
use pqueue_postgres::{PostgresBackend, PostgresRelationalBackend};

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_hot_projection_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn explicit_decline_transition() -> CommitTransition {
    CommitTransition {
        request_id: Some(RequestId::new("txn-explicit-decline").unwrap()),
        entries: vec![CommitTransitionEntry {
            claim_ref: ClaimRef {
                item_id: ItemId::new("1").unwrap(),
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: pqueue_conformance::ts(1),
                item_version: 0,
            },
            finalize: FinalizeKind::Complete,
            side_records: vec![SideRecord {
                key: b"state/run".to_vec(),
                payload: bytes::Bytes::from_static(b"opaque"),
            }],
            lifecycle_items: vec![],
            instance_fence: None,
        }],
    }
}

fn assert_commit_transition_is_explicitly_declined<B>(backend: &B)
where
    B: Backend + CommitTransitionPort + RecoveryReadPort,
{
    assert_eq!(backend.commit_capabilities(), CommitCapabilities::default());

    let transition = explicit_decline_transition();
    let err = futures::executor::block_on(backend.commit_transition(
        &qkey(),
        transition.clone(),
        pqueue_conformance::ts(2),
        None,
    ))
    .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let err = futures::executor::block_on(
        backend.explain_commit(&qkey(), transition.request_id.clone().unwrap()),
    )
    .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let err = futures::executor::block_on(backend.side_record(&qkey(), b"state/run")).unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
}

fn assert_commit_transition_is_supported(backend: &PostgresRelationalBackend) {
    assert!(backend.commit_capabilities().atomic_transition_commit);
    assert!(backend.commit_capabilities().vectorized_commit);
    assert!(backend.commit_capabilities().retained_commit_idempotency);

    let q = qkey();
    futures::executor::block_on(async {
        backend.create_queue(qdef()).await.unwrap();
        let transition = CommitTransition {
            request_id: Some(RequestId::new("txn-supported").unwrap()),
            entries: vec![],
        };
        let outcomes = backend
            .commit_transition(&q, transition.clone(), pqueue_conformance::ts(0), None)
            .await
            .unwrap();
        assert!(outcomes.is_empty());
        let recovery = backend
            .explain_commit(&q, transition.request_id.clone().unwrap())
            .await
            .unwrap()
            .expect("retained replay record");
        assert!(recovery.entries.is_empty());
        assert!(
            backend
                .side_record(&q, b"state/run")
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn hot_projection_capabilities_are_explicit() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES HOT PROJECTION SKIPPED (hot_projection_capabilities_are_explicit) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };

    let backend = PostgresBackend::connect_in_schema(&url, &fresh_schema())
        .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
    let flags = backend.hot_projection_capabilities(&qkey());
    assert_eq!(flags, pqueue::QueryCapabilityFlags::default());
    assert!(!flags.side_record_query);

    let err = futures::executor::block_on(backend.range_scan(
        &qkey(),
        RangeScanRequest {
            index: None,
            filters: vec![],
            order_by: vec![],
            page_size: 1,
            cursor: None,
        },
    ))
    .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
}

#[test]
fn commit_transition_capabilities_are_explicit() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES COMMIT-TRANSITION SKIPPED (commit_transition_capabilities_are_explicit) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };

    let postgres = PostgresBackend::connect_in_schema(&url, &fresh_schema())
        .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
    assert_commit_transition_is_explicitly_declined(&postgres);

    let relational = PostgresRelationalBackend::connect_in_schema(&url, &fresh_schema())
        .expect("connect postgres-relational (is PQUEUE_PG_TEST_URL a live DB?)");
    assert_commit_transition_is_supported(&relational);
}
