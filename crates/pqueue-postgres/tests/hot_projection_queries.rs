//! Env-gated hot-projection capability smoke test for the postgres adapter.
//!
//! Without `PQUEUE_PG_TEST_URL` this prints a loud skip and returns green, so local CI does not depend
//! on a live database. When configured, it proves the adapter advertises hot-projection support
//! explicitly rather than pretending the capability exists.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue::RangeScanRequest;
use pqueue_conformance::qkey;
use pqueue_engine::{EngineError, HotProjectionQueryPort};
use pqueue_postgres::PostgresBackend;

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_hot_projection_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
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
