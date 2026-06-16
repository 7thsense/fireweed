#![forbid(unsafe_code)]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use pqueue_client::{
    BatchClaimResponse, ItemResultStatus, NativeRoute, QueueAdminStateResponse,
    RenewLeasesResponse, RepairItemsResponse,
};
use pqueue_service::{AuthContext, app};
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
async fn operator_repair_tests_pause_resume_admin_state_controls_claims_only() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let paused_app = app.clone();

    let pause = json_request(
        NativeRoute::PauseQueue,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "pause-1",
            "reason": "operator maintenance"
        }),
    );
    let response = app.clone().oneshot(pause).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let paused: QueueAdminStateResponse = decode(response).await;
    assert!(paused.paused);
    assert!(paused.queue_admin_paused);
    assert!(!paused.eligible_age_accrues);

    let claim = json_request(
        NativeRoute::BatchClaim,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "claim-paused",
            "worker_id": "worker-a",
            "max_items": 10,
            "lease_duration_ms": 300000
        }),
    );
    let response = paused_app.clone().oneshot(claim).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let claim: BatchClaimResponse = decode(response).await;
    assert!(claim.queue_paused);
    assert!(claim.items.is_empty());

    let push = json_request(
        NativeRoute::BatchPush,
        "tenant-a",
        "queue-a",
        serde_json::json!({"request_id":"push-while-paused","items":[]}),
    );
    assert_eq!(
        paused_app.clone().oneshot(push).await.unwrap().status(),
        StatusCode::OK
    );

    let finalize = json_request(
        NativeRoute::BatchFinalize,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "finalize-while-paused",
            "finalizations": [{
                "item_id": "item-a",
                "lease_token": "lease-a",
                "outcome": "complete"
            }]
        }),
    );
    assert_eq!(
        paused_app.clone().oneshot(finalize).await.unwrap().status(),
        StatusCode::OK
    );

    let resume = json_request(
        NativeRoute::ResumeQueue,
        "tenant-a",
        "queue-a",
        serde_json::json!({"request_id": "resume-1"}),
    );
    let response = paused_app.clone().oneshot(resume).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let resumed: QueueAdminStateResponse = decode(response).await;
    assert!(!resumed.paused);
    assert!(!resumed.queue_admin_paused);
    assert!(resumed.eligible_age_accrues);
    assert!(resumed.command_position > paused.command_position);

    let claim = json_request(
        NativeRoute::BatchClaim,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "claim-resumed",
            "worker_id": "worker-a",
            "max_items": 10,
            "lease_duration_ms": 300000
        }),
    );
    let response = paused_app.oneshot(claim).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let claim: BatchClaimResponse = decode(response).await;
    assert!(!claim.queue_paused);
}

#[tokio::test]
async fn operator_repair_tests_repair_items_fences_lease_and_bumps_version() {
    let app = app(AuthContext::new("operator-a", ["tenant-a"]));
    let repair = json_request(
        NativeRoute::RepairItems,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "repair-1",
            "action": "force_release",
            "items": [{
                "item_id": "item-a",
                "lease_token": "lease-a"
            }]
        }),
    );
    let response = app.clone().oneshot(repair).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let repair: RepairItemsResponse = decode(response).await;
    assert!(repair.inv11_lease_fence_checked);
    assert!(repair.force_release_preserves_progress_clock);
    assert_eq!(repair.results[0].status, ItemResultStatus::Repaired);
    assert_eq!(repair.results[0].detail.as_deref(), Some("force_release"));
    assert_eq!(repair.results[0].item_version, Some(1));

    let renew = json_request(
        NativeRoute::RenewLeases,
        "tenant-a",
        "queue-a",
        serde_json::json!({
            "request_id": "renew-fenced",
            "items": [{
                "item_id": "item-a",
                "lease_token": "lease-a",
                "lease_duration_ms": 300000
            }]
        }),
    );
    let response = app.oneshot(renew).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let renew: RenewLeasesResponse = decode(response).await;
    assert_eq!(renew.results[0].status, ItemResultStatus::StaleLease);
    assert!(
        renew.results[0]
            .detail
            .as_deref()
            .unwrap()
            .contains("operator action")
    );
}
