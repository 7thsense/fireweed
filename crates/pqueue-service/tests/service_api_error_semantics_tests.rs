#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ApiErrorCode, ClaimUnit, NativeRoute, ProblemDetails};
use pqueue_service::{AuthContext, app};
use tower::ServiceExt;

fn test_app() -> axum::Router {
    app(AuthContext::new("principal-a", ["tenant-a"]))
}

async fn problem_body(response: axum::response::Response) -> ProblemDetails {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn service_api_error_semantics_tests_malformed_json_uses_problem_details() {
    let request = Request::builder()
        .method(NativeRoute::BatchPush.method())
        .uri(NativeRoute::BatchPush.path("tenant-a", Some("queue-a")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
    assert_eq!(problem.status, StatusCode::BAD_REQUEST.as_u16());
    assert_eq!(
        problem.type_uri,
        "https://pqueue.dev/problems/invalid-request"
    );
}

#[tokio::test]
async fn service_api_error_semantics_tests_unknown_route_uses_problem_details() {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/tenants/tenant-a/not-api-001")
        .body(Body::from("{}"))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
    assert_eq!(problem.status, StatusCode::NOT_FOUND.as_u16());
}

#[tokio::test]
async fn service_api_error_semantics_tests_api_001_routes_are_registered() {
    let request = Request::builder()
        .method(NativeRoute::BatchClaim.method())
        .uri(NativeRoute::BatchClaim.path("tenant-a", Some("queue-a")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({
                "request_id": "req-claim-route",
                "worker_id": "worker-a",
                "max_items": 10,
                "lease_duration_ms": 300000
            })
            .to_string(),
        ))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let routed: pqueue_client::BatchClaimResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(routed.request_id, "req-claim-route");
    assert_eq!(routed.claim_unit, ClaimUnit::Item);
}
