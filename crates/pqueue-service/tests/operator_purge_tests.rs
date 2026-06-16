#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ItemResultStatus, NativeRoute, OperatorItemsResponse, OperatorOperationState};
use pqueue_service::{AuthContext, app};
use tower::ServiceExt;

fn json_request(
    route: NativeRoute,
    tenant: &str,
    queue: &str,
    body: serde_json::Value,
) -> Request<Body> {
    Request::builder()
        .method(route.method())
        .uri(route.path(tenant, Some(queue)))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn decode<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn operator_purge_tests_dry_run_is_exact_side_effect_free_and_replay_safe() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let request = json_request(
        NativeRoute::OperatorPurgeItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "purge-dry-run",
            "dry_run": true,
            "cohort_whole": true,
            "expected_match_count": 2,
            "item_refs": [
                {"item_id": "stale-a"},
                {"item_id": "stale-b"}
            ]
        }),
    );

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let purge: OperatorItemsResponse = decode(response).await;

    assert_eq!(purge.state, OperatorOperationState::Succeeded);
    assert!(purge.dry_run);
    assert!(purge.side_effect_free);
    assert!(purge.multi_shard_converged);
    assert!(purge.idempotent_replay);
    assert!(purge.cohort_whole);
    assert_eq!(purge.progress.matched, 2);
    assert_eq!(purge.progress.affected, 0);
    assert_eq!(purge.results.len(), 2);
    assert_eq!(purge.results[0].status, ItemResultStatus::Purged);
    assert!(
        purge.results[0]
            .detail
            .as_deref()
            .unwrap()
            .contains("side_effect_free=true")
    );
}

#[tokio::test]
async fn operator_purge_tests_archive_is_idempotent_and_retention_records_policy_enforcement() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let archive = json_request(
        NativeRoute::ArchiveItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "archive-1",
            "cohort_whole": true,
            "item_refs": [{"item_id": "complete-a"}]
        }),
    );

    let response = app.clone().oneshot(archive).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let archive: OperatorItemsResponse = decode(response).await;
    assert!(archive.archive_idempotent);
    assert!(archive.idempotent_replay);
    assert!(archive.multi_shard_converged);
    assert!(archive.cohort_whole);
    assert_eq!(archive.results[0].status, ItemResultStatus::Archived);
    assert_eq!(
        archive.results[0].detail.as_deref(),
        Some("archive_idempotent=true")
    );

    let retention = json_request(
        NativeRoute::RunRetention,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "retention-1",
            "expected_match_count": 3
        }),
    );

    let response = app.oneshot(retention).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let retention: OperatorItemsResponse = decode(response).await;
    assert!(retention.retention_policy_enforced);
    assert!(retention.idempotent_replay);
    assert_eq!(retention.progress.matched, 3);
    assert_eq!(retention.progress.affected, 3);
}
