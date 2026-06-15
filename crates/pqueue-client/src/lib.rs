#![forbid(unsafe_code)]

use std::collections::BTreeMap;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GateState {
    Blocked,
    Open,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetGate {
    pub gate_key: String,
    pub state: GateState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetGatesRequest {
    pub request_id: String,
    pub gates: Vec<SetGate>,
}

impl SetGatesRequest {
    pub fn canonical_gates(&self) -> Vec<SetGate> {
        let mut gates = std::collections::BTreeMap::new();
        for gate in &self.gates {
            gates.insert(gate.gate_key.clone(), gate.state);
        }
        gates
            .into_iter()
            .map(|(gate_key, state)| SetGate { gate_key, state })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateShardStatus {
    pub shard: String,
    pub applied_command_position: u64,
    pub converged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetGatesResponse {
    pub request_id: String,
    pub gate_epoch: u64,
    pub gates: Vec<SetGate>,
    pub shards: Vec<GateShardStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupCompleteness {
    WholeEligible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupBatching {
    pub max_groups: u32,
    pub group_completeness: GroupCompleteness,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClaimCompatibility {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub same_group_key: Option<bool>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata_equals: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_batching: Option<GroupBatching>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whole_cohort: Option<bool>,
}

impl ClaimCompatibility {
    pub fn has_same_group_key_filter(&self) -> bool {
        self.same_group_key.unwrap_or(false)
    }

    pub fn is_whole_cohort(&self) -> bool {
        self.whole_cohort.unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchClaimRequest {
    pub request_id: String,
    pub worker_id: String,
    pub max_items: u32,
    pub lease_duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<ClaimCompatibility>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimUnit {
    Item,
    SameGroupKey,
    WholeGroup,
    WholeCohort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedItem {
    pub item_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchClaimResponse {
    pub request_id: String,
    pub claim_unit: ClaimUnit,
    pub items: Vec<ClaimedItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claimed_group_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_basis: Option<String>,
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

    #[test]
    fn set_gates_requests_canonicalize_last_write_wins() {
        let request = SetGatesRequest {
            request_id: "req-gates".to_string(),
            gates: vec![
                SetGate {
                    gate_key: "z".to_string(),
                    state: GateState::Blocked,
                },
                SetGate {
                    gate_key: "a".to_string(),
                    state: GateState::Open,
                },
                SetGate {
                    gate_key: "z".to_string(),
                    state: GateState::Open,
                },
            ],
        };

        assert_eq!(
            request.canonical_gates(),
            vec![
                SetGate {
                    gate_key: "a".to_string(),
                    state: GateState::Open,
                },
                SetGate {
                    gate_key: "z".to_string(),
                    state: GateState::Open,
                },
            ]
        );
    }

    #[test]
    fn claim_compatibility_distinguishes_filter_from_whole_group_unit() {
        let same_group_filter = ClaimCompatibility {
            same_group_key: Some(true),
            ..ClaimCompatibility::default()
        };
        assert!(same_group_filter.has_same_group_key_filter());
        assert!(same_group_filter.group_batching.is_none());

        let whole_group = ClaimCompatibility {
            group_batching: Some(GroupBatching {
                max_groups: 3,
                group_completeness: GroupCompleteness::WholeEligible,
            }),
            ..ClaimCompatibility::default()
        };
        assert!(!whole_group.has_same_group_key_filter());
        assert_eq!(whole_group.group_batching.unwrap().max_groups, 3);
    }
}
