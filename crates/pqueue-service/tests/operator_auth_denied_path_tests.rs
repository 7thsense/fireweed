#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{
    ApiErrorCode, ItemResultStatus, NativeRoute, OperatorItemsResponse, ProblemDetails,
    RepairItemsResponse,
};
use pqueue_service::{AuthContext, RedactedLeaseToken, app};
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

async fn decode<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn operator_auth_denied_path_tests_distinguish_operator_and_tenant_denials() {
    let data_plane = app(AuthContext::new("principal-a", ["tenant-a"]));
    let request = json_request(
        NativeRoute::RedriveItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "redrive-denied",
            "retry_count_mode": "reset",
            "item_refs": [{"item_id": "failed-a"}]
        }),
    );
    let response = data_plane.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem: ProblemDetails = decode(response).await;
    assert_eq!(problem.code, ApiErrorCode::OperatorForbidden);

    let operator = app(AuthContext::new("operator-a", ["tenant-a"]));
    let request = json_request(
        NativeRoute::ArchiveItems,
        "tenant-b",
        "queue-a",
        serde_json::json!({
            "request_id": "cross-tenant",
            "item_refs": [{"item_id": "complete-a"}]
        }),
    );
    let response = operator.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let problem: ProblemDetails = decode(response).await;
    assert_eq!(problem.code, ApiErrorCode::QueueForbidden);
}

#[test]
fn operator_auth_denied_path_tests_redact_lease_tokens_in_logs() {
    let token = RedactedLeaseToken::new("lease-secret-token");
    assert_eq!(format!("{token}"), "[redacted]");
    assert_eq!(format!("{token:?}"), "LeaseToken([redacted])");
    assert!(!format!("{token:?}").contains("lease-secret-token"));
}

#[tokio::test]
async fn operator_auth_denied_path_tests_record_cohort_wholeness_across_operator_actions() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let repair = json_request(
        NativeRoute::RepairItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "repair-cohort",
            "action": "force_retry",
            "cohort_whole": true,
            "items": [{"item_id": "item-a"}]
        }),
    );
    let response = app.clone().oneshot(repair).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let repair: RepairItemsResponse = decode(response).await;
    assert!(repair.cohort_whole);
    assert_eq!(repair.results[0].status, ItemResultStatus::Repaired);

    for (route, status, request_id, extra) in [
        (
            NativeRoute::RedriveItems,
            ItemResultStatus::Redriven,
            "redrive-cohort",
            serde_json::json!({"retry_count_mode": "increment"}),
        ),
        (
            NativeRoute::OperatorPurgeItems,
            ItemResultStatus::Purged,
            "purge-cohort",
            serde_json::json!({}),
        ),
        (
            NativeRoute::ArchiveItems,
            ItemResultStatus::Archived,
            "archive-cohort",
            serde_json::json!({}),
        ),
    ] {
        let mut body = serde_json::json!({
            "request_id": request_id,
            "cohort_whole": true,
            "item_refs": [{"item_id": format!("{request_id}-item")}]
        });
        let body_obj = body.as_object_mut().unwrap();
        for (key, value) in extra.as_object().unwrap() {
            body_obj.insert(key.clone(), value.clone());
        }
        let response = app
            .clone()
            .oneshot(json_request(route, "tenant-a", "queue-a", body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let action: OperatorItemsResponse = decode(response).await;
        assert!(action.cohort_whole);
        assert_eq!(action.results[0].status, status);
    }
}
