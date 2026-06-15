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
            "cohort-enabled",
            QueueCapabilities {
                group_co_residency: true,
                cohort_policy_enabled: true,
                cohort_completion_bound_ms: Some(30_000),
                progress_bound_ms: Some(60_000),
                ..QueueCapabilities::default()
            },
        )
        .with_queue(
            "tenant-a",
            "cohort-disabled",
            QueueCapabilities {
                group_co_residency: true,
                cohort_policy_enabled: false,
                progress_bound_ms: Some(60_000),
                ..QueueCapabilities::default()
            },
        )
        .with_queue(
            "tenant-a",
            "bad-bound",
            QueueCapabilities {
                group_co_residency: true,
                cohort_policy_enabled: true,
                cohort_completion_bound_ms: Some(90_000),
                progress_bound_ms: Some(60_000),
                ..QueueCapabilities::default()
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
async fn service_cohort_tests_whole_cohort_claim_uses_atomic_claim_unit() {
    let body = serde_json::json!({
        "request_id": "req-cohort-claim",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": { "whole_cohort": true }
    });

    let response = test_app()
        .oneshot(claim_request("cohort-enabled", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let claim: BatchClaimResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(claim.request_id, "req-cohort-claim");
    assert_eq!(claim.claim_unit, ClaimUnit::WholeCohort);
    assert!(claim.items.is_empty());
    assert!(claim.cohort_id.is_none());
    assert!(claim.cohort_lease_token.is_none());
}

#[tokio::test]
async fn service_cohort_tests_rejects_whole_cohort_when_policy_disabled() {
    let body = serde_json::json!({
        "request_id": "req-disabled",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": { "whole_cohort": true }
    });

    let response = test_app()
        .oneshot(claim_request("cohort-disabled", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
}

#[tokio::test]
async fn service_cohort_tests_rejects_conflicting_member_leak_modes() {
    let body = serde_json::json!({
        "request_id": "req-conflict",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": {
            "whole_cohort": true,
            "same_group_key": true
        }
    });

    let response = test_app()
        .oneshot(claim_request("cohort-enabled", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
}

#[tokio::test]
async fn service_cohort_tests_rejects_completion_bound_over_progress_bound() {
    let body = serde_json::json!({
        "request_id": "req-bad-bound",
        "worker_id": "worker-a",
        "max_items": 500,
        "lease_duration_ms": 300000,
        "compatibility": { "whole_cohort": true }
    });

    let response = test_app()
        .oneshot(claim_request("bad-bound", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
}

#[tokio::test]
async fn service_cohort_tests_cross_tenant_denied_before_body_parse() {
    let request = Request::builder()
        .method(NativeRoute::BatchClaim.method())
        .uri(NativeRoute::BatchClaim.path("tenant-b", Some("cohort-enabled")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::QueueForbidden);
}
