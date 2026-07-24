use std::time::{SystemTime, UNIX_EPOCH};

use fireweed_core::{FilterOp, MetricsByQueryRequest, QueryFilter, TypedValue};
use fireweed_engine::HotProjectionQueryPort;
use fireweed_sqlite::{
    SqliteRelationalBackend, composed_sqlite_backend, composed_sqlite_backend_in_memory,
};

fn transition_metrics_request() -> MetricsByQueryRequest {
    MetricsByQueryRequest {
        index: Some("by_record_kind_scheduled_at".to_string()),
        filters: vec![QueryFilter {
            field: "record_kind".to_string(),
            op: FilterOp::Eq,
            value: TypedValue::String("transition".to_string()),
        }],
    }
}

#[tokio::test]
async fn composed_sqlite_filtered_lifecycle_metrics_conformance() {
    fireweed_conformance::scenarios::filtered_lifecycle_metrics_are_exact_and_read_only(|| {
        composed_sqlite_backend_in_memory().expect("open composed sqlite")
    })
    .await;
}

#[tokio::test]
async fn relational_sqlite_filtered_lifecycle_metrics_conformance() {
    fireweed_conformance::scenarios::filtered_lifecycle_metrics_are_exact_and_read_only(|| {
        SqliteRelationalBackend::in_memory().expect("open relational sqlite")
    })
    .await;
}

#[tokio::test]
async fn composed_sqlite_filtered_metrics_survive_reopen() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("pqueue-filtered-metrics-{nonce}.sqlite"));
    let path_string = path.to_string_lossy().into_owned();
    fireweed_conformance::scenarios::filtered_lifecycle_metrics_are_exact_and_read_only(|| {
        composed_sqlite_backend(&path_string).expect("open composed sqlite")
    })
    .await;

    let reopened = composed_sqlite_backend(&path_string).expect("reopen composed sqlite");
    let metrics = reopened
        .metrics_by_query(&fireweed_conformance::shard(), transition_metrics_request())
        .await
        .unwrap();
    assert_eq!(
        (
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ),
        (1, 1, 1, 1)
    );
    drop(reopened);
    let _ = std::fs::remove_file(path);
}
