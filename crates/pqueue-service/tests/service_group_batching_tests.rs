#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ApiErrorCode, BatchClaimResponse, ClaimUnit, NativeRoute, ProblemDetails};
use pqueue_service::{AuthContext, QueueCapabilities, QueueCatalog, app_with_queue_catalog};
use tower::ServiceExt;

fn test_app() -> axum::Router {
    let catalog = QueueCatalog::new()
        .with_queue(
            "tenant-a",
            "grouped",
            QueueCapabilities {
                group_co_residency: true,
                max_eligible_group_size: Some(50),
            },
        )
        .with_queue(
            "tenant-a",
            "plain",
            QueueCapabilities {
                group_co_residency: false,
                max_eligible_group_size: None,
            },
        );
    app_with_queue_catalog(AuthContext::new("principal-a", ["tenant-a"]), catalog)
}

async fn problem_body(response: axum::response::Response) -> ProblemDetails {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn claim_request(queue_id: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(NativeRoute::BatchClaim.method())
        .uri(NativeRoute::BatchClaim.path("tenant-a", Some(queue_id)))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn service_group_batching_tests_whole_group_claim_uses_summary_basis() {
    let body = serde_json::json!({
        "request_id": "req-group-claim",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": {
            "group_batching": {
                "max_groups": 10,
                "group_completeness": "whole_eligible"
            },
            "metadata_equals": { "connector": "marketo" }
        }
    });

    let response = test_app()
        .oneshot(claim_request("grouped", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let claim: BatchClaimResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(claim.request_id, "req-group-claim");
    assert_eq!(claim.claim_unit, ClaimUnit::WholeGroup);
    assert_eq!(claim.summary_basis.as_deref(), Some("pqueue_group_summary"));
    assert!(claim.items.is_empty());
}

#[tokio::test]
async fn service_group_batching_tests_same_group_key_stays_item_filter() {
    let body = serde_json::json!({
        "request_id": "req-same-group",
        "worker_id": "worker-a",
        "max_items": 100,
        "lease_duration_ms": 300000,
        "compatibility": {
            "same_group_key": true,
            "metadata_equals": { "connector": "marketo" }
        }
    });

    let response = test_app()
        .oneshot(claim_request("plain", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let claim: BatchClaimResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(claim.claim_unit, ClaimUnit::SameGroupKey);
    assert!(claim.summary_basis.is_none());
}

#[tokio::test]
async fn service_group_batching_tests_rejects_non_co_resident_group_batching() {
    let body = serde_json::json!({
        "request_id": "req-bad-grouping",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": {
            "group_batching": {
                "max_groups": 10,
                "group_completeness": "whole_eligible"
            }
        }
    });

    let response = test_app()
        .oneshot(claim_request("plain", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
}

#[tokio::test]
async fn service_group_batching_tests_rejects_conflicting_claim_modes() {
    let body = serde_json::json!({
        "request_id": "req-conflict",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": {
            "same_group_key": true,
            "group_batching": {
                "max_groups": 10,
                "group_completeness": "whole_eligible"
            }
        }
    });

    let response = test_app()
        .oneshot(claim_request("grouped", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
}

#[tokio::test]
async fn service_group_batching_tests_cross_tenant_denied_before_body_parse() {
    let request = Request::builder()
        .method(NativeRoute::BatchClaim.method())
        .uri(NativeRoute::BatchClaim.path("tenant-b", Some("grouped")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::QueueForbidden);
}
