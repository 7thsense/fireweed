//! Container runtime contract tests for the `pqueue-service` entrypoint surface.
//!
//! These exercise the health router that the production container image exposes
//! for Kubernetes liveness/readiness probes plus the full service router built
//! from runtime configuration.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use pqueue_service::runtime::{
    LIVENESS_PATH, READINESS_PATH, RuntimeConfig, health_router, service_router,
};
use tower::ServiceExt;

async fn body_text(body: Body) -> String {
    let bytes = to_bytes(body, 1024).await.expect("body should be readable");
    String::from_utf8(bytes.to_vec()).expect("body should be utf8")
}

#[tokio::test]
async fn liveness_probe_returns_ok() {
    let response = health_router()
        .oneshot(
            Request::builder()
                .uri(LIVENESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response.into_body()).await, "ok");
}

#[tokio::test]
async fn readiness_probe_returns_ready() {
    let response = health_router()
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response.into_body()).await, "ready");
}

#[tokio::test]
async fn service_router_serves_health_alongside_api() {
    let config = RuntimeConfig::from_getter(|_| None).expect("defaults are valid");
    let router = service_router(pqueue_service::AppState::new(config.auth_context()));

    let health = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(LIVENESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(health.status(), StatusCode::OK);

    // An unknown API route still falls through to the API-001 not-found handler,
    // proving the health routes merged without displacing the app fallback.
    let unknown = router
        .oneshot(
            Request::builder()
                .uri("/v1/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
}
