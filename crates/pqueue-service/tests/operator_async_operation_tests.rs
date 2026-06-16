#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{
    ApiErrorCode, NativeRoute, OperatorItemsResponse, OperatorOperationState, ProblemDetails,
};
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

fn operation_request(
    route: NativeRoute,
    tenant: &str,
    queue: &str,
    operation_id: &str,
) -> Request<Body> {
    Request::builder()
        .method(route.method())
        .uri(route.operation_path(tenant, queue, operation_id))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .unwrap()
}

async fn decode<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn operator_async_operation_tests_replay_request_id_returns_one_operation_id() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let body = serde_json::json!({
        "request_id": "redrive-replay",
        "retry_count_mode": "preserve",
        "expected_match_count": 4,
        "item_refs": [
            {"item_id": "failed-a"},
            {"item_id": "failed-b"},
            {"item_id": "failed-c"},
            {"item_id": "failed-d"}
        ]
    });

    let mut operation_id = String::new();
    for _ in 0..100 {
        let response = app
            .clone()
            .oneshot(json_request(
                NativeRoute::RedriveItems,
                "tenant-a",
                "queue-a",
                body.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let operation: OperatorItemsResponse = decode(response).await;
        if operation_id.is_empty() {
            operation_id = operation.operation_id.clone();
        }
        assert_eq!(operation.operation_id, operation_id);
        assert_eq!(operation.state, OperatorOperationState::Succeeded);
        assert_eq!(operation.progress.matched, 4);
        assert_eq!(operation.progress.affected, 4);
        assert_eq!(operation.progress.failed, 0);
    }

    let response = app
        .clone()
        .oneshot(operation_request(
            NativeRoute::GetOperation,
            "tenant-a",
            "queue-a",
            &operation_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let operation: OperatorItemsResponse = decode(response).await;
    assert_eq!(operation.operation_id, operation_id);
    assert_eq!(operation.state, OperatorOperationState::Succeeded);
    assert_eq!(operation.progress.shards_total, 1);
    assert_eq!(operation.progress.shards_complete, 1);
    assert_eq!(operation.progress.matched, 4);
    assert_eq!(operation.progress.affected, 4);
    assert_eq!(operation.progress.failed, 0);
}

#[tokio::test]
async fn operator_async_operation_tests_replayed_request_id_with_changed_body_conflicts() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let first = json_request(
        NativeRoute::OperatorPurgeItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "purge-replay-conflict",
            "item_refs": [{"item_id": "stale-a"}]
        }),
    );
    let response = app.clone().oneshot(first).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let changed = json_request(
        NativeRoute::OperatorPurgeItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "purge-replay-conflict",
            "item_refs": [{"item_id": "stale-b"}]
        }),
    );
    let response = app.oneshot(changed).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let problem: ProblemDetails = decode(response).await;
    assert_eq!(problem.code, ApiErrorCode::RequestIdConflict);
}

#[tokio::test]
async fn operator_async_operation_tests_cancel_preserves_committed_shard_counts() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let response = app
        .clone()
        .oneshot(json_request(
            NativeRoute::ArchiveItems,
            "tenant-a",
            "queue-a",
            serde_json::json!({
                "request_id": "archive-cancel",
                "expected_match_count": 3,
                "item_refs": [
                    {"item_id": "complete-a"},
                    {"item_id": "complete-b"},
                    {"item_id": "complete-c"}
                ]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let operation: OperatorItemsResponse = decode(response).await;
    assert_eq!(operation.progress.matched, 3);
    assert_eq!(operation.progress.affected, 3);
    assert_eq!(operation.progress.failed, 0);

    let response = app
        .clone()
        .oneshot(operation_request(
            NativeRoute::CancelOperation,
            "tenant-a",
            "queue-a",
            &operation.operation_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let canceled: OperatorItemsResponse = decode(response).await;
    assert_eq!(canceled.state, OperatorOperationState::Canceled);
    assert_eq!(canceled.progress.matched, operation.progress.matched);
    assert_eq!(canceled.progress.affected, operation.progress.affected);
    assert_eq!(canceled.progress.failed, operation.progress.failed);

    let response = app
        .oneshot(operation_request(
            NativeRoute::GetOperation,
            "tenant-a",
            "queue-a",
            &operation.operation_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let fetched: OperatorItemsResponse = decode(response).await;
    assert_eq!(fetched.state, OperatorOperationState::Canceled);
    assert_eq!(fetched.progress.affected, 3);
}
