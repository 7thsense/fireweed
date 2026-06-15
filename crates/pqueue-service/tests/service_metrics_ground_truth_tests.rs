#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use pqueue_client::{
    ApiTimestamp, GetQueueMetricsResponse, LifecycleCounts, NativeRoute, ProblemDetails,
    QueueMetrics,
};
use pqueue_service::{AuthContext, QueueCatalog, QueueMetricsSnapshot, app_with_queue_catalog};
use tower::ServiceExt;

fn ts(seconds: i64) -> ApiTimestamp {
    ApiTimestamp {
        seconds,
        nanoseconds: 0,
    }
}

fn test_app() -> axum::Router {
    let catalog = QueueCatalog::new().with_metrics(
        "tenant-a",
        "queue-a",
        QueueMetricsSnapshot {
            as_of: ts(1_718_000_100),
            metrics: QueueMetrics {
                lifecycle_counts: LifecycleCounts {
                    pending: 7,
                    leased: 2,
                    complete: 5,
                    failed: 1,
                },
                retry_backlog: 1,
                oldest_eligible_age_ms: Some(30_000),
                progress_bound_risk_count: 3,
                active_leases: 2,
                recurring_pending: 4,
                recurring_leased: 1,
            },
        },
    );
    app_with_queue_catalog(AuthContext::new("principal-a", ["tenant-a"]), catalog)
}

async fn problem_body(response: axum::response::Response) -> ProblemDetails {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn service_metrics_ground_truth_tests_returns_exact_oldest_age_and_counts() {
    let request = Request::builder()
        .method(NativeRoute::GetQueueMetrics.method())
        .uri(NativeRoute::GetQueueMetrics.path("tenant-a", Some("queue-a")))
        .body(Body::empty())
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let metrics: GetQueueMetricsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(metrics.queue_id, "queue-a");
    assert_eq!(metrics.as_of, ts(1_718_000_100));
    assert!(metrics.exact_oldest_eligible_age);
    assert_eq!(metrics.metrics.oldest_eligible_age_ms, Some(30_000));
    assert_eq!(metrics.metrics.lifecycle_counts.pending, 7);
    assert_eq!(metrics.metrics.active_leases, 2);
    assert_eq!(metrics.metrics.progress_bound_risk_count, 3);
    assert_eq!(metrics.metrics.recurring_pending, 4);
    assert_eq!(metrics.metrics.recurring_leased, 1);
}

#[tokio::test]
async fn service_metrics_ground_truth_tests_empty_queue_omits_oldest_age() {
    let request = Request::builder()
        .method(NativeRoute::GetQueueMetrics.method())
        .uri(NativeRoute::GetQueueMetrics.path("tenant-a", Some("empty")))
        .body(Body::empty())
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let metrics: GetQueueMetricsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(metrics.metrics.oldest_eligible_age_ms, None);
    assert_eq!(metrics.metrics.lifecycle_counts.pending, 0);
}

#[tokio::test]
async fn service_metrics_ground_truth_tests_cross_tenant_denied() {
    let request = Request::builder()
        .method(NativeRoute::GetQueueMetrics.method())
        .uri(NativeRoute::GetQueueMetrics.path("tenant-b", Some("queue-a")))
        .body(Body::empty())
        .unwrap();

    let response = test_app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem = problem_body(response).await;
    assert_eq!(problem.code, pqueue_client::ApiErrorCode::QueueForbidden);
}
