#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

pub const PROBLEM_TYPE_BASE: &str = "https://pqueue.dev/problems";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiErrorCode {
    QueueNotFound,
    QueueForbidden,
    QueueDefinitionConflict,
    RequestIdConflict,
    RequestExpired,
    InvalidRequest,
    BatchTooLarge,
    CommitTimeout,
    GatesNotEnabled,
    RateLimit,
}

impl ApiErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueNotFound => "queue-not-found",
            Self::QueueForbidden => "queue-forbidden",
            Self::QueueDefinitionConflict => "queue-definition-conflict",
            Self::RequestIdConflict => "request-id-conflict",
            Self::RequestExpired => "request-expired",
            Self::InvalidRequest => "invalid-request",
            Self::BatchTooLarge => "batch-too-large",
            Self::CommitTimeout => "commit-timeout",
            Self::GatesNotEnabled => "gates-not-enabled",
            Self::RateLimit => "rate-limit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub type_uri: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub code: ApiErrorCode,
}

impl ProblemDetails {
    pub fn new(code: ApiErrorCode, status: u16, detail: impl Into<String>) -> Self {
        Self {
            type_uri: format!("{PROBLEM_TYPE_BASE}/{}", code.as_str()),
            title: code.as_str().to_string(),
            status,
            detail: detail.into(),
            code,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeRoute {
    CreateQueue,
    BatchPush,
    BatchUpdate,
    SetGates,
    BatchClaim,
    PurgeItems,
    RenewLeases,
    BatchFinalize,
    GetQueueMetrics,
    DiscoverActiveScopes,
}

impl NativeRoute {
    pub const fn method(self) -> &'static str {
        match self {
            Self::GetQueueMetrics => "GET",
            _ => "POST",
        }
    }

    pub const fn operation(self) -> &'static str {
        match self {
            Self::CreateQueue => "CreateQueue",
            Self::BatchPush => "BatchPush",
            Self::BatchUpdate => "BatchUpdate",
            Self::SetGates => "SetGates",
            Self::BatchClaim => "BatchClaim",
            Self::PurgeItems => "PurgeItems",
            Self::RenewLeases => "BatchRenewLeases",
            Self::BatchFinalize => "BatchFinalize",
            Self::GetQueueMetrics => "GetQueueMetrics",
            Self::DiscoverActiveScopes => "DiscoverActiveScopes",
        }
    }

    pub fn path(self, tenant_id: &str, queue_id: Option<&str>) -> String {
        match self {
            Self::CreateQueue => format!("/v1/tenants/{tenant_id}/queues"),
            Self::DiscoverActiveScopes => format!("/v1/tenants/{tenant_id}/scopes:discover"),
            Self::BatchPush => format!(
                "/v1/tenants/{tenant_id}/queues/{}/items:push",
                queue_id.expect("queue_id is required for BatchPush")
            ),
            Self::BatchUpdate => format!(
                "/v1/tenants/{tenant_id}/queues/{}/items:update",
                queue_id.expect("queue_id is required for BatchUpdate")
            ),
            Self::SetGates => format!(
                "/v1/tenants/{tenant_id}/queues/{}/gates:set",
                queue_id.expect("queue_id is required for SetGates")
            ),
            Self::BatchClaim => format!(
                "/v1/tenants/{tenant_id}/queues/{}/items:claim",
                queue_id.expect("queue_id is required for BatchClaim")
            ),
            Self::PurgeItems => format!(
                "/v1/tenants/{tenant_id}/queues/{}/items:purge",
                queue_id.expect("queue_id is required for PurgeItems")
            ),
            Self::RenewLeases => format!(
                "/v1/tenants/{tenant_id}/queues/{}/leases:renew",
                queue_id.expect("queue_id is required for RenewLeases")
            ),
            Self::BatchFinalize => format!(
                "/v1/tenants/{tenant_id}/queues/{}/items:finalize",
                queue_id.expect("queue_id is required for BatchFinalize")
            ),
            Self::GetQueueMetrics => format!(
                "/v1/tenants/{tenant_id}/queues/{}/metrics",
                queue_id.expect("queue_id is required for GetQueueMetrics")
            ),
        }
    }
}

pub mod scaffold {
    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_error_codes_keep_contract_spelling() {
        assert_eq!(ApiErrorCode::QueueForbidden.as_str(), "queue-forbidden");
        assert_eq!(ApiErrorCode::InvalidRequest.as_str(), "invalid-request");
    }

    #[test]
    fn native_routes_render_api_001_paths() {
        assert_eq!(
            NativeRoute::BatchClaim.path("tenant-a", Some("queue-a")),
            "/v1/tenants/tenant-a/queues/queue-a/items:claim"
        );
        assert_eq!(NativeRoute::BatchClaim.method(), "POST");
        assert_eq!(
            NativeRoute::DiscoverActiveScopes.path("tenant-a", None),
            "/v1/tenants/tenant-a/scopes:discover"
        );
    }
}
