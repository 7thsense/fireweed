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
    pub cohort_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cohort_lease_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_basis: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiTimestamp {
    pub seconds: i64,
    #[serde(default)]
    pub nanoseconds: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizeOutcome {
    Complete,
    Fail,
    Retry,
    Release,
    Rearm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RearmOptions {
    pub not_before: ApiTimestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cohort_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cohort_lease_token: Option<String>,
    pub outcome: FinalizeOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rearm: Option<RearmOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchFinalizeRequest {
    pub request_id: String,
    pub finalizations: Vec<FinalizeItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemResultStatus {
    Completed,
    Failed,
    Retried,
    Released,
    Rearmed,
    Purged,
    NotFound,
    Invalid,
    Conflict,
    StaleLease,
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_item_key: Option<String>,
    pub status: ItemResultStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_position: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchFinalizeResponse {
    pub request_id: String,
    pub results: Vec<ItemResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_item_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeItemsRequest {
    pub request_id: String,
    #[serde(default)]
    pub force: bool,
    pub items: Vec<PurgeItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeItemsResponse {
    pub request_id: String,
    pub results: Vec<ItemResult>,
    pub tombstone_replay_safe: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_retention_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleCounts {
    pub pending: u64,
    pub leased: u64,
    pub complete: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetrics {
    pub lifecycle_counts: LifecycleCounts,
    pub retry_backlog: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_eligible_age_ms: Option<u64>,
    pub progress_bound_risk_count: u64,
    pub active_leases: u64,
    pub recurring_pending: u64,
    pub recurring_leased: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetQueueMetricsResponse {
    pub queue_id: String,
    pub as_of: ApiTimestamp,
    pub metrics: QueueMetrics,
    pub exact_oldest_eligible_age: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryGranularity {
    Queue,
    Group,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverActiveScopesRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub granularity: Option<DiscoveryGranularity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_results: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveScope {
    pub queue_id: String,
    #[serde(default)]
    pub group_key: Option<String>,
    pub oldest_eligible_age_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eligible_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_bound_risk_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverActiveScopesResponse {
    pub as_of: ApiTimestamp,
    pub active_scopes: Vec<ActiveScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    pub read_only: bool,
    pub summary_basis: String,
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

    #[test]
    fn batch_claim_response_carries_whole_cohort_fields() {
        let response = BatchClaimResponse {
            request_id: "req-cohort".to_string(),
            claim_unit: ClaimUnit::WholeCohort,
            items: vec![ClaimedItem {
                item_id: "item-a".to_string(),
                group_key: Some("callback-42".to_string()),
                lease_token: None,
            }],
            claimed_group_keys: vec![],
            cohort_id: Some("cohort-a".to_string()),
            cohort_lease_token: Some("cohort-lease-a".to_string()),
            summary_basis: None,
        };

        assert_eq!(response.claim_unit, ClaimUnit::WholeCohort);
        assert_eq!(response.cohort_id.as_deref(), Some("cohort-a"));
        assert_eq!(
            response.cohort_lease_token.as_deref(),
            Some("cohort-lease-a")
        );
    }

    #[test]
    fn recurrence_and_purge_dtos_keep_api_status_names() {
        let finalize = BatchFinalizeResponse {
            request_id: "req-finalize".to_string(),
            results: vec![ItemResult {
                item_id: Some("item-a".to_string()),
                client_item_key: None,
                status: ItemResultStatus::Rearmed,
                detail: None,
                command_position: Some(7),
            }],
        };
        assert_eq!(finalize.results[0].status, ItemResultStatus::Rearmed);

        let purge = PurgeItemsResponse {
            request_id: "req-purge".to_string(),
            results: vec![ItemResult {
                item_id: None,
                client_item_key: Some("key-a".to_string()),
                status: ItemResultStatus::Purged,
                detail: None,
                command_position: Some(8),
            }],
            tombstone_replay_safe: true,
            tombstone_retention_ms: Some(86_400_000),
        };
        assert!(purge.tombstone_replay_safe);
        assert_eq!(purge.results[0].status, ItemResultStatus::Purged);
    }

    #[test]
    fn discovery_and_metrics_dtos_represent_exact_oldest_age() {
        let metrics = GetQueueMetricsResponse {
            queue_id: "queue-a".to_string(),
            as_of: ApiTimestamp {
                seconds: 1_718_000_100,
                nanoseconds: 0,
            },
            metrics: QueueMetrics {
                lifecycle_counts: LifecycleCounts {
                    pending: 2,
                    leased: 1,
                    complete: 0,
                    failed: 0,
                },
                retry_backlog: 0,
                oldest_eligible_age_ms: Some(20_000),
                progress_bound_risk_count: 1,
                active_leases: 1,
                recurring_pending: 0,
                recurring_leased: 0,
            },
            exact_oldest_eligible_age: true,
        };
        assert_eq!(metrics.metrics.oldest_eligible_age_ms, Some(20_000));
        assert!(metrics.exact_oldest_eligible_age);

        let discovery = DiscoverActiveScopesResponse {
            as_of: metrics.as_of,
            active_scopes: vec![ActiveScope {
                queue_id: "queue-a".to_string(),
                group_key: Some("group-a".to_string()),
                oldest_eligible_age_ms: 20_000,
                eligible_count: Some(2),
                progress_bound_risk_count: Some(1),
            }],
            next_page_token: None,
            read_only: true,
            summary_basis: "pqueue_group_summary".to_string(),
        };
        assert_eq!(discovery.active_scopes[0].oldest_eligible_age_ms, 20_000);
        assert!(discovery.read_only);
    }
}
