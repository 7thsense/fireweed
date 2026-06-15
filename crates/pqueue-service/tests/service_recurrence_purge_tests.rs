#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{
    BatchFinalizeResponse, ItemResultStatus, NativeRoute, ProblemDetails, PurgeItemsResponse,
};
use pqueue_service::{AuthContext, QueueCapabilities, QueueCatalog, app_with_queue_catalog};
use tower::ServiceExt;

fn test_app() -> axum::Router {
    let catalog = QueueCatalog::new()
        .with_queue(
            "tenant-a",
            "recurring",
            QueueCapabilities {
                recurring: true,
                recurrence_until_seconds: Some(1_718_100_000),
                client_item_key_retention_ms: Some(86_400_000),
                ..QueueCapabilities::default()
            },
        )
        .with_queue(
            "tenant-a",
            "oneshot",
            QueueCapabilities {
                recurring: false,
                client_item_key_retention_ms: Some(86_400_000),
                ..QueueCapabilities::default()
            },
        );
    app_with_queue_catalog(AuthContext::new("principal-a", ["tenant-a"]), catalog)
}

async fn problem_body(response: axum::response::Response) -> ProblemDetails {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn finalize_request(queue_id: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(NativeRoute::BatchFinalize.method())
        .uri(NativeRoute::BatchFinalize.path("tenant-a", Some(queue_id)))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn purge_request(queue_id: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(NativeRoute::PurgeItems.method())
        .uri(NativeRoute::PurgeItems.path("tenant-a", Some(queue_id)))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn service_recurrence_purge_tests_rearm_requires_recurring_queue_and_not_before() {
    let body = serde_json::json!({
        "request_id": "req-rearm",
        "finalizations": [{
            "item_id": "tick-1",
            "lease_token": "lease-1",
            "outcome": "rearm",
            "rearm": { "not_before": { "seconds": 1718000100 } }
        }]
    });

    let response = test_app()
        .oneshot(finalize_request("recurring", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let finalize: BatchFinalizeResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(finalize.results[0].status, ItemResultStatus::Rearmed);
    assert_eq!(finalize.results[0].command_position, Some(0));
}

#[tokio::test]
async fn service_recurrence_purge_tests_rearm_on_oneshot_is_per_item_invalid() {
    let body = serde_json::json!({
        "request_id": "req-rearm-oneshot",
        "finalizations": [{
            "item_id": "tick-1",
            "lease_token": "lease-1",
            "outcome": "rearm",
            "rearm": { "not_before": { "seconds": 1718000100 } }
        }]
    });

    let response = test_app()
        .oneshot(finalize_request("oneshot", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let finalize: BatchFinalizeResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(finalize.results[0].status, ItemResultStatus::Invalid);
}

#[tokio::test]
async fn service_recurrence_purge_tests_rearm_missing_or_after_until_is_per_item_failure() {
    let missing = serde_json::json!({
        "request_id": "req-rearm-missing",
        "finalizations": [{
            "item_id": "tick-1",
            "lease_token": "lease-1",
            "outcome": "rearm"
        }]
    });
    let response = test_app()
        .oneshot(finalize_request("recurring", missing))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let finalize: BatchFinalizeResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(finalize.results[0].status, ItemResultStatus::Invalid);

    let after_until = serde_json::json!({
        "request_id": "req-rearm-terminal",
        "finalizations": [{
            "item_id": "tick-1",
            "lease_token": "lease-1",
            "outcome": "rearm",
            "rearm": { "not_before": { "seconds": 1718200000 } }
        }]
    });
    let response = test_app()
        .oneshot(finalize_request("recurring", after_until))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let finalize: BatchFinalizeResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(finalize.results[0].status, ItemResultStatus::Terminal);
}

#[tokio::test]
async fn service_recurrence_purge_tests_purge_force_records_tombstone_replay_surface() {
    let body = serde_json::json!({
        "request_id": "req-purge",
        "force": true,
        "items": [{ "client_item_key": "key-tick-1" }]
    });

    let response = test_app()
        .oneshot(purge_request("recurring", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let purge: PurgeItemsResponse = serde_json::from_slice(&body).unwrap();
    assert!(purge.tombstone_replay_safe);
    assert_eq!(purge.tombstone_retention_ms, Some(86_400_000));
    assert_eq!(purge.results[0].status, ItemResultStatus::Purged);
    assert_eq!(purge.results[0].command_position, Some(0));
}

#[tokio::test]
async fn service_recurrence_purge_tests_purge_without_force_or_target_is_per_item_failure() {
    let conflict = serde_json::json!({
        "request_id": "req-purge-conflict",
        "force": false,
        "items": [{ "item_id": "tick-1" }]
    });
    let response = test_app()
        .oneshot(purge_request("recurring", conflict))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let purge: PurgeItemsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(purge.results[0].status, ItemResultStatus::Conflict);

    let invalid = serde_json::json!({
        "request_id": "req-purge-invalid",
        "force": true,
        "items": [{}]
    });
    let response = test_app()
        .oneshot(purge_request("recurring", invalid))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let purge: PurgeItemsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(purge.results[0].status, ItemResultStatus::Invalid);
}

#[tokio::test]
async fn service_recurrence_purge_tests_cross_tenant_denied_before_body_parse() {
    let request = Request::builder()
        .method(NativeRoute::PurgeItems.method())
        .uri(NativeRoute::PurgeItems.path("tenant-b", Some("recurring")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, pqueue_client::ApiErrorCode::QueueForbidden);
}
