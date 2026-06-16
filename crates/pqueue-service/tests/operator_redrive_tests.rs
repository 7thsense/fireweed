#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ItemResultStatus, NativeRoute, OperatorItemsResponse};
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
async fn operator_redrive_tests_failed_items_become_eligible_with_recorded_convergence() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let request = json_request(
        NativeRoute::RedriveItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "redrive-1",
            "cohort_whole": true,
            "item_refs": [{
                "item_id": "failed-a"
            }],
            "not_before": {
                "seconds": 200,
                "nanoseconds": 0
            },
            "retry_count_mode": "reset"
        }),
    );

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let redrive: OperatorItemsResponse = decode(response).await;

    assert_eq!(redrive.request_id, "redrive-1");
    assert!(redrive.operation_id.ends_with("/redrive/redrive-1"));
    assert!(redrive.multi_shard_converged);
    assert!(redrive.idempotent_replay);
    assert!(redrive.cohort_whole);
    assert_eq!(redrive.progress.shards_total, 1);
    assert_eq!(redrive.progress.shards_complete, 1);
    assert_eq!(redrive.progress.matched, 1);
    assert_eq!(redrive.progress.affected, 1);
    assert_eq!(redrive.results[0].item_id.as_deref(), Some("failed-a"));
    assert_eq!(redrive.results[0].status, ItemResultStatus::Redriven);
    let detail = redrive.results[0].detail.as_deref().unwrap();
    assert!(detail.contains("eligible_since=max(commit=0,redrive.not_before=200)"));
    assert!(detail.contains("retry_count_mode=reset"));
}

#[tokio::test]
async fn operator_redrive_tests_requires_retry_count_mode_and_targets() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let request = json_request(
        NativeRoute::RedriveItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "redrive-missing-mode",
            "item_refs": [{"item_id": "failed-a"}]
        }),
    );

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let request = json_request(
        NativeRoute::RedriveItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "redrive-missing-target",
            "retry_count_mode": "preserve",
            "item_refs": []
        }),
    );

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
