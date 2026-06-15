#![forbid(unsafe_code)]

use std::time::Instant;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ApiErrorCode, GateState, NativeRoute, ProblemDetails, SetGatesResponse};
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
async fn service_gate_tests_set_gates_route_canonicalizes_and_reports_convergence() {
    let body = serde_json::json!({
        "request_id": "req-gates-1",
        "gates": [
            {"gate_key": "z", "state": "blocked"},
            {"gate_key": "a", "state": "open"},
            {"gate_key": "z", "state": "open"}
        ]
    });
    let request = Request::builder()
        .method(NativeRoute::SetGates.method())
        .uri(NativeRoute::SetGates.path("tenant-a", Some("queue-a")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let started = Instant::now();
    let response = test_app().oneshot(request).await.unwrap();
    let elapsed = started.elapsed();
    eprintln!(
        "AC-LAT-2 service SetGates route elapsed_ms={}",
        elapsed.as_millis()
    );

    assert_eq!(response.status(), StatusCode::OK);
    assert!(elapsed.as_millis() < 1_000);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let set_gates: SetGatesResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(set_gates.request_id, "req-gates-1");
    assert_eq!(set_gates.gate_epoch, 1);
    assert_eq!(set_gates.gates.len(), 2);
    assert_eq!(set_gates.gates[0].gate_key, "a");
    assert_eq!(set_gates.gates[0].state, GateState::Open);
    assert_eq!(set_gates.gates[1].gate_key, "z");
    assert_eq!(set_gates.gates[1].state, GateState::Open);
    assert_eq!(set_gates.shards.len(), 1);
    assert!(set_gates.shards[0].converged);
}

#[tokio::test]
async fn service_gate_tests_empty_gate_batch_is_invalid_request() {
    let request = Request::builder()
        .method(NativeRoute::SetGates.method())
        .uri(NativeRoute::SetGates.path("tenant-a", Some("queue-a")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"request_id":"req-empty","gates":[]}"#))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::InvalidRequest);
}

#[tokio::test]
async fn service_gate_tests_cross_tenant_set_gates_denied_before_body_parse() {
    let request = Request::builder()
        .method(NativeRoute::SetGates.method())
        .uri(NativeRoute::SetGates.path("tenant-b", Some("queue-a")))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, ApiErrorCode::QueueForbidden);
}
