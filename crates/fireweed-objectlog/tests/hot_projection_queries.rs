//! Env-gated hot-projection capability smoke test for the object-log adapter.
//!
//! Without `FIREWEED_OBJECTLOG_TEST_ROOT` this prints a loud skip and returns green, so local CI does not
//! depend on a configured filesystem root. When configured, it proves the adapter advertises hot-
//! projection support explicitly rather than pretending the capability exists.

use fireweed_core::{ItemId, LeaseToken, QueryCapabilityFlags, RangeScanRequest, RequestId};
use fireweed_engine::{
    Backend, ClaimRef, CommitCapabilities, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, EngineError, FinalizeKind, HotProjectionQueryPort, RecoveryReadPort,
};
use fireweed_objectlog::ObjectLogBackend;

fn skip_loudly() {
    eprintln!(
        "OBJECTLOG HOT PROJECTION SKIPPED (hot_projection_capabilities_are_explicit) — set FIREWEED_OBJECTLOG_TEST_ROOT to a writable root"
    );
}

fn explicit_decline_transition() -> CommitTransition {
    CommitTransition {
        request_id: Some(RequestId::new("txn-explicit-decline").unwrap()),
        entries: vec![CommitTransitionEntry {
            claim_ref: ClaimRef {
                item_id: ItemId::new("1").unwrap(),
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: fireweed_conformance::ts(1),
                item_version: 0,
            },
            additional_claim_refs: Vec::new(),
            finalize: FinalizeKind::Complete,
            side_records: vec![],
            lifecycle_items: vec![],
            instance_fence: None,
        }],
    }
}

async fn assert_commit_transition_is_explicitly_declined<B>(backend: &B)
where
    B: Backend + CommitTransitionPort + RecoveryReadPort,
{
    assert_eq!(backend.commit_capabilities(), CommitCapabilities::default());

    let transition = explicit_decline_transition();
    let err = backend
        .commit_transition(
            &fireweed_conformance::qkey(),
            transition.clone(),
            fireweed_conformance::ts(2),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let err = backend
        .explain_commit(
            &fireweed_conformance::qkey(),
            transition.request_id.clone().unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let err = backend
        .side_record(&fireweed_conformance::qkey(), b"state/run")
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
}

#[tokio::test]
async fn hot_projection_capabilities_are_explicit() {
    let Ok(root) = std::env::var("FIREWEED_OBJECTLOG_TEST_ROOT") else {
        skip_loudly();
        return;
    };

    let backend = ObjectLogBackend::open(root).expect("open object-log backend");
    let q = fireweed_conformance::qkey();
    let flags = backend.hot_projection_capabilities(&q);
    assert_eq!(flags, QueryCapabilityFlags::default());
    assert!(!flags.side_record_query);

    let err = backend
        .range_scan(
            &q,
            RangeScanRequest {
                index: None,
                filters: vec![],
                order_by: vec![],
                page_size: 1,
                cursor: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
}

#[tokio::test]
async fn commit_transition_capabilities_are_explicit() {
    let Ok(root) = std::env::var("FIREWEED_OBJECTLOG_TEST_ROOT") else {
        skip_loudly();
        return;
    };

    let backend = ObjectLogBackend::open(root).expect("open object-log backend");
    assert_commit_transition_is_explicitly_declined(&backend).await;
}
