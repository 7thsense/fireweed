#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ApiErrorCode, NativeRoute, ProblemDetails};
use pqueue_service::{AuthContext, RedactedLeaseToken, app, hash_lease_token};
use tower::ServiceExt;

async fn problem_body(response: axum::response::Response) -> ProblemDetails {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn service_auth_tenant_tests_authorized_tenant_reaches_route_handler() {
    let request = Request::builder()
        .method(NativeRoute::CreateQueue.method())
        .uri(NativeRoute::CreateQueue.path("tenant-a", None))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = app(AuthContext::new("principal-a", ["tenant-a"]))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let routed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(routed["principal_id"], "principal-a");
    assert_eq!(routed["tenant_id"], "tenant-a");
}

#[tokio::test]
async fn service_auth_tenant_tests_cross_tenant_request_is_forbidden_before_body_parse() {
    let request = Request::builder()
        .method(NativeRoute::BatchPush.method())
        .uri(NativeRoute::BatchPush.path("tenant-b", Some("queue-a")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();

    let response = app(AuthContext::new("principal-a", ["tenant-a"]))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::QueueForbidden);
    assert_eq!(problem.status, StatusCode::FORBIDDEN.as_u16());
}

#[test]
fn service_auth_tenant_tests_lease_tokens_are_hashed_and_redacted() {
    let plaintext = "plain-worker-lease-token";
    let hash = hash_lease_token(plaintext);
    assert_eq!(hash.len(), 32);
    assert_ne!(hash.as_slice(), plaintext.as_bytes());

    let redacted = RedactedLeaseToken::new(plaintext);
    assert_eq!(redacted.hash(), hash);
    assert_eq!(format!("{redacted}"), "[redacted]");
    assert_eq!(format!("{redacted:?}"), "LeaseToken([redacted])");
    assert!(!format!("{redacted:?}").contains(plaintext));
}
