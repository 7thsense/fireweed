//! Env-gated hot-projection capability smoke test for the object-log adapter.
//!
//! Without `PQUEUE_OBJECTLOG_TEST_ROOT` this prints a loud skip and returns green, so local CI does not
//! depend on a configured filesystem root. When configured, it proves the adapter advertises hot-
//! projection support explicitly rather than pretending the capability exists.

use pqueue_core::{QueryCapabilityFlags, RangeScanRequest};
use pqueue_engine::{EngineError, HotProjectionQueryPort};
use pqueue_objectlog::ObjectLogBackend;

fn skip_loudly() {
    eprintln!(
        "OBJECTLOG HOT PROJECTION SKIPPED (hot_projection_capabilities_are_explicit) — set PQUEUE_OBJECTLOG_TEST_ROOT to a writable root"
    );
}

#[tokio::test]
async fn hot_projection_capabilities_are_explicit() {
    let Ok(root) = std::env::var("PQUEUE_OBJECTLOG_TEST_ROOT") else {
        skip_loudly();
        return;
    };

    let backend = ObjectLogBackend::open(root).expect("open object-log backend");
    let q = pqueue_conformance::qkey();
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
