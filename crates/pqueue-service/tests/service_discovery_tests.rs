#![forbid(unsafe_code)]

use std::time::Instant;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{ApiTimestamp, DiscoverActiveScopesResponse, NativeRoute, ProblemDetails};
use pqueue_service::{ActiveScopeSnapshot, AuthContext, QueueCatalog, app_with_queue_catalog};
use tower::ServiceExt;

fn ts(seconds: i64) -> ApiTimestamp {
    ApiTimestamp {
        seconds,
        nanoseconds: 0,
    }
}

fn test_app() -> axum::Router {
    let catalog = QueueCatalog::new()
        .with_active_scope(
            ActiveScopeSnapshot::new(
                "tenant-a",
                "queue-a",
                Some("group-new".to_string()),
                10_000,
                ts(1_718_000_100),
            )
            .with_counts(Some(2), Some(0)),
        )
        .with_active_scope(
            ActiveScopeSnapshot::new(
                "tenant-a",
                "queue-a",
                Some("group-old".to_string()),
                30_000,
                ts(1_718_000_090),
            )
            .with_counts(Some(1), Some(1)),
        )
        .with_active_scope(
            ActiveScopeSnapshot::new("tenant-a", "queue-b", None, 20_000, ts(1_718_000_095))
                .with_counts(Some(3), Some(1)),
        )
        .with_active_scope(
            ActiveScopeSnapshot::new(
                "tenant-b",
                "queue-foreign",
                Some("group-x".to_string()),
                99_000,
                ts(1_718_000_095),
            )
            .with_counts(Some(9), Some(9)),
        );
    app_with_queue_catalog(AuthContext::new("principal-a", ["tenant-a"]), catalog)
}

async fn problem_body(response: axum::response::Response) -> ProblemDetails {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn discover_request(body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(NativeRoute::DiscoverActiveScopes.method())
        .uri(NativeRoute::DiscoverActiveScopes.path("tenant-a", None))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn service_discovery_tests_group_top_n_is_sorted_and_exact() {
    let body = serde_json::json!({
        "queue_id": "queue-a",
        "granularity": "group",
        "max_results": 2
    });

    let started = Instant::now();
    let response = test_app().oneshot(discover_request(body)).await.unwrap();
    let elapsed = started.elapsed();
    eprintln!(
        "AC-LAT-3 service DiscoverActiveScopes elapsed_ms={}",
        elapsed.as_millis()
    );

    assert_eq!(response.status(), StatusCode::OK);
    assert!(elapsed.as_millis() < 1_000);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let discovery: DiscoverActiveScopesResponse = serde_json::from_slice(&body).unwrap();
    assert!(discovery.read_only);
    assert_eq!(discovery.summary_basis, "pqueue_group_summary");
    assert_eq!(discovery.as_of, ts(1_718_000_090));
    assert_eq!(discovery.active_scopes.len(), 2);
    assert_eq!(
        discovery.active_scopes[0].group_key.as_deref(),
        Some("group-old")
    );
    assert_eq!(discovery.active_scopes[0].oldest_eligible_age_ms, 30_000);
    assert_eq!(
        discovery.active_scopes[1].group_key.as_deref(),
        Some("group-new")
    );
}

#[tokio::test]
async fn service_discovery_tests_group_filter_and_queue_rollup_are_auth_filtered() {
    let filtered = serde_json::json!({
        "queue_id": "queue-a",
        "granularity": "group",
        "group_key": "group-new",
        "max_results": 10
    });
    let response = test_app()
        .oneshot(discover_request(filtered))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let discovery: DiscoverActiveScopesResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(discovery.active_scopes.len(), 1);
    assert_eq!(
        discovery.active_scopes[0].group_key.as_deref(),
        Some("group-new")
    );

    let rollup = serde_json::json!({
        "granularity": "queue",
        "max_results": 10
    });
    let response = test_app().oneshot(discover_request(rollup)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let discovery: DiscoverActiveScopesResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(discovery.active_scopes.len(), 2);
    assert_eq!(discovery.active_scopes[0].queue_id, "queue-a");
    assert_eq!(discovery.active_scopes[0].oldest_eligible_age_ms, 30_000);
    assert!(
        discovery
            .active_scopes
            .iter()
            .all(|s| s.queue_id != "queue-foreign")
    );
}

#[tokio::test]
async fn service_discovery_tests_group_granularity_requires_queue_and_auth_before_parse() {
    let response = test_app()
        .oneshot(discover_request(serde_json::json!({
            "granularity": "group",
            "max_results": 10
        })))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let request = Request::builder()
        .method(NativeRoute::DiscoverActiveScopes.method())
        .uri(NativeRoute::DiscoverActiveScopes.path("tenant-b", None))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not-json"))
        .unwrap();
    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, pqueue_client::ApiErrorCode::QueueForbidden);
}
