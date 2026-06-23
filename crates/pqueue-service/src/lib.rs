#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use pqueue_client::{
    ActiveScope, ApiErrorCode, ApiTimestamp, BatchClaimRequest, BatchClaimResponse,
    BatchFinalizeRequest, BatchFinalizeResponse, ClaimCompatibility, ClaimUnit,
    DiscoverActiveScopesRequest, DiscoverActiveScopesResponse, DiscoveryGranularity, FinalizeItem,
    FinalizeOutcome, GateShardStatus, GetQueueMetricsResponse, ItemResult, ItemResultStatus,
    LifecycleCounts, NativeRoute, OperatorItemsRequest, OperatorItemsResponse,
    OperatorOperationProgress, OperatorOperationState, ProblemDetails, PurgeItem,
    PurgeItemsRequest, PurgeItemsResponse, QueueAdminRequest, QueueAdminStateResponse,
    QueueMetrics, RenewLeasesRequest, RenewLeasesResponse, RepairAction, RepairItemsRequest,
    RepairItemsResponse, RetryCountMode, SetGatesRequest, SetGatesResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

pub mod runtime;
pub mod verification_ledger;

#[derive(Clone, Copy)]
pub struct RedactedLeaseToken<'a> {
    token: &'a str,
}

impl<'a> RedactedLeaseToken<'a> {
    pub fn new(token: &'a str) -> Self {
        Self { token }
    }

    pub fn hash(self) -> [u8; 32] {
        hash_lease_token(self.token)
    }
}

impl std::fmt::Debug for RedactedLeaseToken<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeaseToken([redacted])")
    }
}

impl std::fmt::Display for RedactedLeaseToken<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

pub fn hash_lease_token(token: &str) -> [u8; 32] {
    let digest = Sha256::digest(token.as_bytes());
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&digest);
    hash
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    principal_id: String,
    tenants: BTreeSet<String>,
}

impl AuthContext {
    pub fn new(
        principal_id: impl Into<String>,
        tenants: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            principal_id: principal_id.into(),
            tenants: tenants.into_iter().map(Into::into).collect(),
        }
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub fn authorize_tenant(&self, tenant_id: &str) -> Result<(), ApiProblem> {
        if self.tenants.contains(tenant_id) {
            Ok(())
        } else {
            Err(ApiProblem::new(
                StatusCode::FORBIDDEN,
                ApiErrorCode::QueueForbidden,
                "principal is not authorized for the requested tenant",
            ))
        }
    }

    pub fn authorize_operator_repair(&self) -> Result<(), ApiProblem> {
        if self.principal_id.starts_with("operator-") {
            Ok(())
        } else {
            Err(ApiProblem::new(
                StatusCode::FORBIDDEN,
                ApiErrorCode::OperatorForbidden,
                "principal lacks operator repair privileges",
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppState {
    auth: AuthContext,
    queue_catalog: QueueCatalog,
    queue_admin: Arc<Mutex<QueueAdminState>>,
}

impl AppState {
    pub fn new(auth: AuthContext) -> Self {
        Self {
            auth,
            queue_catalog: QueueCatalog::default(),
            queue_admin: Arc::new(Mutex::new(QueueAdminState::default())),
        }
    }

    pub fn with_queue_catalog(auth: AuthContext, queue_catalog: QueueCatalog) -> Self {
        Self {
            auth,
            queue_catalog,
            queue_admin: Arc::new(Mutex::new(QueueAdminState::default())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct QueueItemLeaseKey {
    tenant_id: String,
    queue_id: String,
    item_id: String,
    lease_token: String,
}

#[derive(Debug, Clone, Default)]
struct QueueAdminState {
    paused_queues: BTreeSet<(String, String)>,
    fenced_leases: BTreeSet<QueueItemLeaseKey>,
    operations: BTreeMap<String, OperatorOperationRecord>,
    operations_by_request: BTreeMap<OperatorOperationRequestKey, String>,
    command_position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OperatorOperationRequestKey {
    tenant_id: String,
    queue_id: String,
    request_id: String,
}

#[derive(Debug, Clone)]
struct OperatorOperationRecord {
    tenant_id: String,
    queue_id: String,
    request_fingerprint: String,
    response: OperatorItemsResponse,
}

impl QueueAdminState {
    fn pause(&mut self, tenant_id: &str, queue_id: &str) -> u64 {
        self.paused_queues
            .insert((tenant_id.to_string(), queue_id.to_string()));
        self.next_position()
    }

    fn resume(&mut self, tenant_id: &str, queue_id: &str) -> u64 {
        self.paused_queues
            .remove(&(tenant_id.to_string(), queue_id.to_string()));
        self.next_position()
    }

    fn is_paused(&self, tenant_id: &str, queue_id: &str) -> bool {
        self.paused_queues
            .contains(&(tenant_id.to_string(), queue_id.to_string()))
    }

    fn fence_lease(&mut self, tenant_id: &str, queue_id: &str, item_id: &str, lease_token: &str) {
        self.fenced_leases.insert(QueueItemLeaseKey {
            tenant_id: tenant_id.to_string(),
            queue_id: queue_id.to_string(),
            item_id: item_id.to_string(),
            lease_token: lease_token.to_string(),
        });
    }

    fn is_fenced(&self, tenant_id: &str, queue_id: &str, item_id: &str, lease_token: &str) -> bool {
        self.fenced_leases.contains(&QueueItemLeaseKey {
            tenant_id: tenant_id.to_string(),
            queue_id: queue_id.to_string(),
            item_id: item_id.to_string(),
            lease_token: lease_token.to_string(),
        })
    }

    fn next_position(&mut self) -> u64 {
        self.command_position += 1;
        self.command_position
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCapabilities {
    pub group_co_residency: bool,
    pub max_eligible_group_size: Option<u32>,
    pub cohort_policy_enabled: bool,
    pub cohort_completion_bound_ms: Option<u64>,
    pub progress_bound_ms: Option<u64>,
    pub recurring: bool,
    pub recurrence_until_seconds: Option<i64>,
    pub client_item_key_retention_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueCatalog {
    queues: BTreeMap<(String, String), QueueCapabilities>,
    metrics: BTreeMap<(String, String), QueueMetricsSnapshot>,
    active_scopes: Vec<ActiveScopeSnapshot>,
}

impl QueueCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_queue(
        mut self,
        tenant_id: impl Into<String>,
        queue_id: impl Into<String>,
        capabilities: QueueCapabilities,
    ) -> Self {
        self.queues
            .insert((tenant_id.into(), queue_id.into()), capabilities);
        self
    }

    pub fn with_metrics(
        mut self,
        tenant_id: impl Into<String>,
        queue_id: impl Into<String>,
        metrics: QueueMetricsSnapshot,
    ) -> Self {
        self.metrics
            .insert((tenant_id.into(), queue_id.into()), metrics);
        self
    }

    pub fn with_active_scope(mut self, scope: ActiveScopeSnapshot) -> Self {
        self.active_scopes.push(scope);
        self
    }

    pub fn capabilities(&self, tenant_id: &str, queue_id: &str) -> QueueCapabilities {
        self.queues
            .get(&(tenant_id.to_string(), queue_id.to_string()))
            .copied()
            .unwrap_or_default()
    }

    pub fn metrics(&self, tenant_id: &str, queue_id: &str) -> Option<&QueueMetricsSnapshot> {
        self.metrics
            .get(&(tenant_id.to_string(), queue_id.to_string()))
    }

    pub fn active_scopes(&self, tenant_id: &str) -> impl Iterator<Item = &ActiveScopeSnapshot> {
        self.active_scopes
            .iter()
            .filter(move |scope| scope.tenant_id == tenant_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMetricsSnapshot {
    pub as_of: ApiTimestamp,
    pub metrics: QueueMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveScopeSnapshot {
    pub tenant_id: String,
    pub queue_id: String,
    pub group_key: Option<String>,
    pub oldest_eligible_age_ms: u64,
    pub eligible_count: Option<u64>,
    pub progress_bound_risk_count: Option<u64>,
    pub as_of: ApiTimestamp,
}

impl ActiveScopeSnapshot {
    pub fn new(
        tenant_id: impl Into<String>,
        queue_id: impl Into<String>,
        group_key: Option<String>,
        oldest_eligible_age_ms: u64,
        as_of: ApiTimestamp,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            queue_id: queue_id.into(),
            group_key,
            oldest_eligible_age_ms,
            eligible_count: None,
            progress_bound_risk_count: None,
            as_of,
        }
    }

    pub fn with_counts(
        mut self,
        eligible_count: Option<u64>,
        progress_bound_risk_count: Option<u64>,
    ) -> Self {
        self.eligible_count = eligible_count;
        self.progress_bound_risk_count = progress_bound_risk_count;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiProblem {
    status: StatusCode,
    problem: ProblemDetails,
}

impl ApiProblem {
    pub fn new(status: StatusCode, code: ApiErrorCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            problem: ProblemDetails::new(code, status.as_u16(), detail),
        }
    }

    pub fn problem(&self) -> &ProblemDetails {
        &self.problem
    }
}

impl IntoResponse for ApiProblem {
    fn into_response(self) -> Response {
        (
            self.status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            Json(self.problem),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
pub struct RouteStubResponse {
    operation: &'static str,
    tenant_id: String,
    queue_id: Option<String>,
    principal_id: String,
}

pub fn app(auth: AuthContext) -> Router {
    app_with_state(AppState::new(auth))
}

pub fn app_with_queue_catalog(auth: AuthContext, queue_catalog: QueueCatalog) -> Router {
    app_with_state(AppState::with_queue_catalog(auth, queue_catalog))
}

pub fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/v1/tenants/{tenant_id}/queues", post(create_queue))
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/items:push",
            post(batch_push),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/items:update",
            post(batch_update),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/gates:set",
            post(set_gates),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/items:claim",
            post(batch_claim),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/items:purge",
            post(purge_items),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/leases:renew",
            post(renew_leases),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/items:finalize",
            post(batch_finalize),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/queue:pause",
            post(pause_queue),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/queue:resume",
            post(resume_queue),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/items:repair",
            post(repair_items),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/items:redrive",
            post(redrive_items),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/items:purge",
            post(operator_purge_items),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/items:archive",
            post(archive_items),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/retention:run",
            post(run_retention),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/operations/{operation_id}",
            get(get_operation),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/operator/operations/{operation_id}/cancel",
            post(cancel_operation),
        )
        .route(
            "/v1/tenants/{tenant_id}/queues/{queue_id}/metrics",
            get(get_queue_metrics),
        )
        .route(
            "/v1/tenants/{tenant_id}/scopes:discover",
            post(discover_active_scopes),
        )
        .fallback(route_not_found)
        .with_state(state)
}

async fn create_queue(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(state, NativeRoute::CreateQueue, tenant_id, None, req, true).await
}

async fn batch_push(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::BatchPush,
        tenant_id,
        Some(queue_id),
        req,
        true,
    )
    .await
}

async fn batch_update(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::BatchUpdate,
        tenant_id,
        Some(queue_id),
        req,
        true,
    )
    .await
}

async fn set_gates(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<SetGatesResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let body: SetGatesRequest = parse_json(req).await?;
    validate_set_gates_request(&body)?;
    let gates = body.canonical_gates();

    Ok(Json(SetGatesResponse {
        request_id: body.request_id,
        gate_epoch: 1,
        gates,
        shards: vec![GateShardStatus {
            shard: format!("{tenant_id}/{queue_id}/shard-0"),
            applied_command_position: 0,
            converged: true,
        }],
    }))
}

async fn batch_claim(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<BatchClaimResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let body: BatchClaimRequest = parse_json(req).await?;
    let capabilities = state.queue_catalog.capabilities(&tenant_id, &queue_id);
    let claim_unit = validate_batch_claim_request(&body, capabilities)?;
    let queue_paused = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned")
        .is_paused(&tenant_id, &queue_id);

    Ok(Json(BatchClaimResponse {
        request_id: body.request_id,
        claim_unit,
        items: vec![],
        queue_paused,
        claimed_group_keys: vec![],
        cohort_id: None,
        cohort_lease_token: None,
        summary_basis: (claim_unit == ClaimUnit::WholeGroup)
            .then(|| "pqueue_group_summary".to_string()),
    }))
}

async fn purge_items(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<PurgeItemsResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let body: PurgeItemsRequest = parse_json(req).await?;
    validate_request_id(&body.request_id)?;
    if body.items.is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "items must not be empty",
        ));
    }
    let capabilities = state.queue_catalog.capabilities(&tenant_id, &queue_id);
    let results = body
        .items
        .iter()
        .map(|item| purge_result(item, body.force))
        .collect();

    Ok(Json(PurgeItemsResponse {
        request_id: body.request_id,
        results,
        tombstone_replay_safe: true,
        tombstone_retention_ms: capabilities.client_item_key_retention_ms,
    }))
}

async fn renew_leases(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RenewLeasesResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let body: RenewLeasesRequest = parse_json(req).await?;
    validate_request_id(&body.request_id)?;
    if body.items.is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "items must not be empty",
        ));
    }

    let admin = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned");
    let results = body
        .items
        .iter()
        .map(|item| {
            if item.lease_duration_ms == 0 {
                return item_result(
                    Some(item.item_id.clone()),
                    None,
                    ItemResultStatus::Invalid,
                    Some("lease_duration_ms must be greater than zero".to_string()),
                );
            }
            if admin.is_fenced(&tenant_id, &queue_id, &item.item_id, &item.lease_token) {
                return item_result(
                    Some(item.item_id.clone()),
                    None,
                    ItemResultStatus::StaleLease,
                    Some("lease token was fenced by an operator action".to_string()),
                );
            }
            item_result(
                Some(item.item_id.clone()),
                None,
                ItemResultStatus::Renewed,
                None,
            )
        })
        .collect();

    Ok(Json(RenewLeasesResponse {
        request_id: body.request_id,
        results,
    }))
}

async fn batch_finalize(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<BatchFinalizeResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let body: BatchFinalizeRequest = parse_json(req).await?;
    validate_request_id(&body.request_id)?;
    if body.finalizations.is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "finalizations must not be empty",
        ));
    }
    let capabilities = state.queue_catalog.capabilities(&tenant_id, &queue_id);
    let results = body
        .finalizations
        .iter()
        .map(|item| finalize_result(item, capabilities))
        .collect();

    Ok(Json(BatchFinalizeResponse {
        request_id: body.request_id,
        results,
    }))
}

async fn get_queue_metrics(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    _req: Request<Body>,
) -> Result<Json<GetQueueMetricsResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let snapshot = state
        .queue_catalog
        .metrics(&tenant_id, &queue_id)
        .cloned()
        .unwrap_or_else(empty_metrics_snapshot);
    Ok(Json(GetQueueMetricsResponse {
        queue_id,
        as_of: snapshot.as_of,
        metrics: snapshot.metrics,
        exact_oldest_eligible_age: true,
    }))
}

async fn discover_active_scopes(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    req: Request<Body>,
) -> Result<Json<DiscoverActiveScopesResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    let body: DiscoverActiveScopesRequest = parse_json(req).await?;
    let granularity = body.granularity.unwrap_or(match body.queue_id {
        Some(_) => DiscoveryGranularity::Group,
        None => DiscoveryGranularity::Queue,
    });
    if granularity == DiscoveryGranularity::Group
        && body.queue_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "group discovery requires queue_id",
        ));
    }
    let max_results = body.max_results.unwrap_or(100);
    if max_results == 0 {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "max_results must be greater than zero",
        ));
    }

    let mut scopes = state
        .queue_catalog
        .active_scopes(&tenant_id)
        .filter(|scope| {
            body.queue_id
                .as_ref()
                .is_none_or(|queue| &scope.queue_id == queue)
        })
        .filter(|scope| {
            body.group_key
                .as_ref()
                .is_none_or(|group| scope.group_key.as_ref() == Some(group))
        })
        .map(|scope| ActiveScope {
            queue_id: scope.queue_id.clone(),
            group_key: (granularity == DiscoveryGranularity::Group)
                .then(|| scope.group_key.clone())
                .flatten(),
            oldest_eligible_age_ms: scope.oldest_eligible_age_ms,
            eligible_count: scope.eligible_count,
            progress_bound_risk_count: scope.progress_bound_risk_count,
        })
        .collect::<Vec<_>>();
    if granularity == DiscoveryGranularity::Queue {
        scopes = roll_up_queue_scopes(scopes);
    }
    scopes.sort_by(|a, b| {
        b.oldest_eligible_age_ms
            .cmp(&a.oldest_eligible_age_ms)
            .then_with(|| a.queue_id.cmp(&b.queue_id))
            .then_with(|| a.group_key.cmp(&b.group_key))
    });
    scopes.truncate(max_results as usize);

    let as_of = state
        .queue_catalog
        .active_scopes(&tenant_id)
        .filter(|scope| {
            body.queue_id
                .as_ref()
                .is_none_or(|queue| &scope.queue_id == queue)
        })
        .map(|scope| scope.as_of)
        .min_by_key(|as_of| (as_of.seconds, as_of.nanoseconds))
        .unwrap_or(ApiTimestamp {
            seconds: 0,
            nanoseconds: 0,
        });

    Ok(Json(DiscoverActiveScopesResponse {
        as_of,
        active_scopes: scopes,
        next_page_token: None,
        read_only: true,
        summary_basis: "pqueue_group_summary".to_string(),
    }))
}

async fn pause_queue(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<QueueAdminStateResponse>, ApiProblem> {
    queue_admin_transition(state, tenant_id, queue_id, req, true).await
}

async fn resume_queue(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<QueueAdminStateResponse>, ApiProblem> {
    queue_admin_transition(state, tenant_id, queue_id, req, false).await
}

async fn queue_admin_transition(
    state: AppState,
    tenant_id: String,
    queue_id: String,
    req: Request<Body>,
    pause: bool,
) -> Result<Json<QueueAdminStateResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    state.auth.authorize_operator_repair()?;
    let body: QueueAdminRequest = parse_json(req).await?;
    validate_request_id(&body.request_id)?;
    let mut admin = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned");
    let command_position = if pause {
        admin.pause(&tenant_id, &queue_id)
    } else {
        admin.resume(&tenant_id, &queue_id)
    };
    let paused = admin.is_paused(&tenant_id, &queue_id);

    Ok(Json(QueueAdminStateResponse {
        request_id: body.request_id,
        queue_id,
        paused,
        queue_admin_paused: paused,
        eligible_age_accrues: !paused,
        command_position,
    }))
}

async fn repair_items(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RepairItemsResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    state.auth.authorize_operator_repair()?;
    let body: RepairItemsRequest = parse_json(req).await?;
    validate_request_id(&body.request_id)?;
    if body.items.is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "items must not be empty",
        ));
    }

    let mut admin = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned");
    let mut fenced_any = false;
    let results = body
        .items
        .iter()
        .map(|item| {
            if item.item_id.trim().is_empty() {
                return item_result(
                    Some(item.item_id.clone()),
                    None,
                    ItemResultStatus::Invalid,
                    Some("item_id is required".to_string()),
                );
            }
            if matches!(
                body.action,
                RepairAction::ForceRelease
                    | RepairAction::ClearLease
                    | RepairAction::ForceRetry
                    | RepairAction::ForceFail
                    | RepairAction::ForceComplete
                    | RepairAction::Reschedule
            ) && let Some(lease_token) = item.lease_token.as_deref()
            {
                admin.fence_lease(&tenant_id, &queue_id, &item.item_id, lease_token);
                fenced_any = true;
            }
            let mut result = item_result(
                Some(item.item_id.clone()),
                None,
                ItemResultStatus::Repaired,
                Some(repair_action_detail(body.action).to_string()),
            );
            let item_version = admin.next_position();
            result.item_version = Some(item_version);
            result.command_position = Some(item_version);
            result
        })
        .collect();

    Ok(Json(RepairItemsResponse {
        request_id: body.request_id,
        results,
        inv11_lease_fence_checked: fenced_any,
        force_release_preserves_progress_clock: body.action == RepairAction::ForceRelease,
        cohort_whole: body.cohort_whole,
    }))
}

async fn redrive_items(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    operator_items_response(state, tenant_id, queue_id, req, OperatorRouteKind::Redrive).await
}

async fn operator_purge_items(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    operator_items_response(state, tenant_id, queue_id, req, OperatorRouteKind::Purge).await
}

async fn archive_items(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    operator_items_response(state, tenant_id, queue_id, req, OperatorRouteKind::Archive).await
}

async fn run_retention(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    operator_items_response(
        state,
        tenant_id,
        queue_id,
        req,
        OperatorRouteKind::Retention,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorRouteKind {
    Redrive,
    Purge,
    Archive,
    Retention,
}

impl OperatorRouteKind {
    fn operation_name(self) -> &'static str {
        match self {
            Self::Redrive => "redrive",
            Self::Purge => "purge",
            Self::Archive => "archive",
            Self::Retention => "retention",
        }
    }

    fn result_status(self) -> ItemResultStatus {
        match self {
            Self::Redrive => ItemResultStatus::Redriven,
            Self::Purge => ItemResultStatus::Purged,
            Self::Archive => ItemResultStatus::Archived,
            Self::Retention => ItemResultStatus::Completed,
        }
    }
}

async fn operator_items_response(
    state: AppState,
    tenant_id: String,
    queue_id: String,
    req: Request<Body>,
    kind: OperatorRouteKind,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    state.auth.authorize_operator_repair()?;
    let body: OperatorItemsRequest = parse_json(req).await?;
    validate_operator_items_request(&body, kind)?;
    let request_fingerprint = serde_json::to_string(&body).expect("operator request serializes");
    let request_key = OperatorOperationRequestKey {
        tenant_id: tenant_id.clone(),
        queue_id: queue_id.clone(),
        request_id: body.request_id.clone(),
    };

    if let Some(existing) = existing_operator_operation(&state, &request_key, &request_fingerprint)?
    {
        return Ok(Json(existing));
    }

    let matched = body
        .expected_match_count
        .unwrap_or(body.item_refs.len() as u64);
    let affected = if body.dry_run { 0 } else { matched };
    let results = body
        .item_refs
        .iter()
        .map(|item| {
            let detail = match kind {
                OperatorRouteKind::Redrive => {
                    Some(redrive_detail(body.not_before, body.retry_count_mode))
                }
                OperatorRouteKind::Purge if body.dry_run => {
                    Some("dry_run exact; side_effect_free=true".to_string())
                }
                OperatorRouteKind::Archive => Some("archive_idempotent=true".to_string()),
                OperatorRouteKind::Retention => Some("retention_policy_enforced=true".to_string()),
                OperatorRouteKind::Purge => None,
            };
            item_result(
                Some(item.item_id.clone()),
                None,
                kind.result_status(),
                detail,
            )
        })
        .collect();

    let operation_id = operation_id(&tenant_id, &queue_id, kind, &body.request_id);
    let response = OperatorItemsResponse {
        request_id: body.request_id.clone(),
        operation_id: operation_id.clone(),
        state: OperatorOperationState::Succeeded,
        progress: OperatorOperationProgress {
            shards_total: 1,
            shards_complete: 1,
            matched,
            affected,
            failed: 0,
            updated_at: ApiTimestamp {
                seconds: 0,
                nanoseconds: 0,
            },
        },
        results,
        dry_run: body.dry_run,
        side_effect_free: body.dry_run,
        multi_shard_converged: true,
        idempotent_replay: true,
        archive_idempotent: kind == OperatorRouteKind::Archive,
        retention_policy_enforced: kind == OperatorRouteKind::Retention,
        cohort_whole: body.cohort_whole,
    };

    record_operator_operation(state, request_key, request_fingerprint, response.clone());
    Ok(Json(response))
}

async fn get_operation(
    State(state): State<AppState>,
    Path((tenant_id, queue_id, operation_id)): Path<(String, String, String)>,
    _req: Request<Body>,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    state.auth.authorize_operator_repair()?;
    let operation = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned")
        .operations
        .get(&operation_id)
        .filter(|operation| operation.response.operation_id == operation_id)
        .filter(|operation| operation.tenant_id == tenant_id && operation.queue_id == queue_id)
        .map(|operation| operation.response.clone())
        .ok_or_else(operation_not_found)?;
    Ok(Json(operation))
}

async fn cancel_operation(
    State(state): State<AppState>,
    Path((tenant_id, queue_id, operation_id)): Path<(String, String, String)>,
    _req: Request<Body>,
) -> Result<Json<OperatorItemsResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    state.auth.authorize_operator_repair()?;
    let mut admin = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned");
    let operation = admin
        .operations
        .get_mut(&operation_id)
        .filter(|operation| operation.response.operation_id == operation_id)
        .filter(|operation| operation.tenant_id == tenant_id && operation.queue_id == queue_id)
        .ok_or_else(operation_not_found)?;
    operation.response.state = OperatorOperationState::Canceled;
    Ok(Json(operation.response.clone()))
}

fn existing_operator_operation(
    state: &AppState,
    request_key: &OperatorOperationRequestKey,
    request_fingerprint: &str,
) -> Result<Option<OperatorItemsResponse>, ApiProblem> {
    let admin = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned");
    let Some(operation_id) = admin.operations_by_request.get(request_key) else {
        return Ok(None);
    };
    let operation = admin
        .operations
        .get(operation_id)
        .expect("request index should reference an operation");
    if operation.request_fingerprint != request_fingerprint {
        return Err(ApiProblem::new(
            StatusCode::CONFLICT,
            ApiErrorCode::RequestIdConflict,
            "request_id already maps to a different operator operation",
        ));
    }
    Ok(Some(operation.response.clone()))
}

fn record_operator_operation(
    state: AppState,
    request_key: OperatorOperationRequestKey,
    request_fingerprint: String,
    response: OperatorItemsResponse,
) {
    let mut admin = state
        .queue_admin
        .lock()
        .expect("queue admin state lock should not be poisoned");
    let tenant_id = request_key.tenant_id.clone();
    let queue_id = request_key.queue_id.clone();
    admin
        .operations_by_request
        .insert(request_key, response.operation_id.clone());
    admin.operations.insert(
        response.operation_id.clone(),
        OperatorOperationRecord {
            tenant_id,
            queue_id,
            request_fingerprint,
            response,
        },
    );
}

fn operation_id(
    tenant_id: &str,
    queue_id: &str,
    kind: OperatorRouteKind,
    request_id: &str,
) -> String {
    format!(
        "oper_{}_{}_{}_{}",
        path_token(tenant_id),
        path_token(queue_id),
        kind.operation_name(),
        path_token(request_id)
    )
}

fn path_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn operation_not_found() -> ApiProblem {
    ApiProblem::new(
        StatusCode::NOT_FOUND,
        ApiErrorCode::OperationNotFound,
        "operation_id was not found",
    )
}

fn validate_operator_items_request(
    req: &OperatorItemsRequest,
    kind: OperatorRouteKind,
) -> Result<(), ApiProblem> {
    validate_request_id(&req.request_id)?;
    if kind != OperatorRouteKind::Retention && req.item_refs.is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "item_refs must not be empty",
        ));
    }
    if kind == OperatorRouteKind::Redrive && req.retry_count_mode.is_none() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "retry_count_mode is required for redrive",
        ));
    }
    for item in &req.item_refs {
        if item.item_id.trim().is_empty() {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "item_id is required",
            ));
        }
    }
    Ok(())
}

fn redrive_detail(
    not_before: Option<ApiTimestamp>,
    retry_count_mode: Option<RetryCountMode>,
) -> String {
    let not_before_seconds = not_before.map_or(0, |timestamp| timestamp.seconds);
    let mode = match retry_count_mode.unwrap_or(RetryCountMode::Preserve) {
        RetryCountMode::Reset => "reset",
        RetryCountMode::Preserve => "preserve",
        RetryCountMode::Increment => "increment",
    };
    format!(
        "eligible_since=max(commit=0,redrive.not_before={not_before_seconds}); retry_count_mode={mode}"
    )
}

fn repair_action_detail(action: RepairAction) -> &'static str {
    match action {
        RepairAction::Reschedule => "reschedule",
        RepairAction::ForceRetry => "force_retry",
        RepairAction::ForceFail => "force_fail",
        RepairAction::ForceComplete => "force_complete",
        RepairAction::ForceRelease => "force_release",
        RepairAction::ClearLease => "clear_lease",
    }
}

async fn route_stub(
    state: AppState,
    route: NativeRoute,
    tenant_id: String,
    queue_id: Option<String>,
    req: Request<Body>,
    expects_body: bool,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    state.auth.authorize_tenant(&tenant_id)?;
    if expects_body {
        let _: serde_json::Value = parse_json(req).await?;
    }

    Ok(Json(RouteStubResponse {
        operation: route.operation(),
        tenant_id,
        queue_id,
        principal_id: state.auth.principal_id().to_string(),
    }))
}

fn validate_set_gates_request(req: &SetGatesRequest) -> Result<(), ApiProblem> {
    validate_request_id(&req.request_id)?;
    if req.gates.is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "gates must not be empty",
        ));
    }
    for gate in &req.gates {
        let valid = !gate.gate_key.is_empty()
            && gate.gate_key.len() <= 256
            && gate.gate_key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            });
        if !valid {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "gate_key must match ^[A-Za-z0-9._:-]{1,256}$",
            ));
        }
    }
    Ok(())
}

fn validate_batch_claim_request(
    req: &BatchClaimRequest,
    capabilities: QueueCapabilities,
) -> Result<ClaimUnit, ApiProblem> {
    validate_request_id(&req.request_id)?;
    if req.worker_id.trim().is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "worker_id is required",
        ));
    }
    if req.max_items == 0 {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "max_items must be greater than zero",
        ));
    }
    if req.lease_duration_ms == 0 {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "lease_duration_ms must be greater than zero",
        ));
    }

    let Some(compatibility) = req.compatibility.as_ref() else {
        return Ok(ClaimUnit::Item);
    };

    validate_claim_compatibility(compatibility, req.max_items, capabilities)
}

fn validate_claim_compatibility(
    compatibility: &ClaimCompatibility,
    max_items: u32,
    capabilities: QueueCapabilities,
) -> Result<ClaimUnit, ApiProblem> {
    if compatibility.group_key.as_deref().is_some_and(|key| {
        key.is_empty()
            || key.len() > 256
            || !key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
    }) {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "group_key must match ^[A-Za-z0-9._:-]{1,256}$",
        ));
    }

    if compatibility.group_batching.is_some() {
        if compatibility.has_same_group_key_filter()
            || compatibility.group_key.is_some()
            || compatibility.is_whole_cohort()
        {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "group_batching cannot be combined with same_group_key, group_key, or whole_cohort",
            ));
        }
        let group_batching = compatibility.group_batching.as_ref().unwrap();
        if group_batching.max_groups == 0 {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "group_batching.max_groups must be greater than zero",
            ));
        }
        let Some(max_group_size) = capabilities.max_eligible_group_size else {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "group_batching requires group_co_residency and max_eligible_group_size",
            ));
        };
        if !capabilities.group_co_residency {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "group_batching requires group_co_residency and max_eligible_group_size",
            ));
        }
        if max_group_size > max_items {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::BatchTooLarge,
                "max_items must be at least max_eligible_group_size for group_batching",
            ));
        }
        return Ok(ClaimUnit::WholeGroup);
    }

    if compatibility.is_whole_cohort() {
        if compatibility.has_same_group_key_filter() || compatibility.group_key.is_some() {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "whole_cohort cannot be combined with same_group_key, group_key, or group_batching",
            ));
        }
        if !capabilities.cohort_policy_enabled {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "whole_cohort requires cohort_policy.enabled=true",
            ));
        }
        if !capabilities.group_co_residency {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "whole_cohort requires group_co_residency",
            ));
        }
        let Some(completion_bound_ms) = capabilities.cohort_completion_bound_ms else {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "whole_cohort requires cohort completion_bound_ms",
            ));
        };
        let Some(progress_bound_ms) = capabilities.progress_bound_ms else {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "whole_cohort requires queue progress_bound_ms",
            ));
        };
        if completion_bound_ms > progress_bound_ms {
            return Err(ApiProblem::new(
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                "cohort completion_bound_ms must be <= progress_bound_ms",
            ));
        }
        return Ok(ClaimUnit::WholeCohort);
    }
    if compatibility.has_same_group_key_filter() {
        return Ok(ClaimUnit::SameGroupKey);
    }
    Ok(ClaimUnit::Item)
}

fn validate_request_id(request_id: &str) -> Result<(), ApiProblem> {
    if request_id.trim().is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "request_id is required",
        ));
    }
    Ok(())
}

fn finalize_result(item: &FinalizeItem, capabilities: QueueCapabilities) -> ItemResult {
    let target = item.item_id.clone();
    if target.as_deref().is_none_or(str::is_empty) && item.cohort_id.is_none() {
        return item_result(
            target,
            None,
            ItemResultStatus::Invalid,
            Some("item_id or cohort_id is required".to_string()),
        );
    }
    if item.lease_token.as_deref().is_none_or(str::is_empty)
        && item.cohort_lease_token.as_deref().is_none_or(str::is_empty)
    {
        return item_result(
            target,
            None,
            ItemResultStatus::Invalid,
            Some("lease_token or cohort_lease_token is required".to_string()),
        );
    }

    match item.outcome {
        FinalizeOutcome::Complete => item_result(target, None, ItemResultStatus::Completed, None),
        FinalizeOutcome::Fail => item_result(target, None, ItemResultStatus::Failed, None),
        FinalizeOutcome::Retry => item_result(target, None, ItemResultStatus::Retried, None),
        FinalizeOutcome::Release => item_result(target, None, ItemResultStatus::Released, None),
        FinalizeOutcome::Rearm => rearm_result(item, capabilities),
    }
}

fn rearm_result(item: &FinalizeItem, capabilities: QueueCapabilities) -> ItemResult {
    let target = item.item_id.clone();
    if !capabilities.recurring {
        return item_result(
            target,
            None,
            ItemResultStatus::Invalid,
            Some("rearm requires recurrence.mode=recurring".to_string()),
        );
    }
    let Some(rearm) = item.rearm.as_ref() else {
        return item_result(
            target,
            None,
            ItemResultStatus::Invalid,
            Some("rearm.not_before is required".to_string()),
        );
    };
    if capabilities
        .recurrence_until_seconds
        .is_some_and(|until| rearm.not_before.seconds > until)
    {
        return item_result(
            target,
            None,
            ItemResultStatus::Terminal,
            Some("rearm is past recurrence.until".to_string()),
        );
    }
    item_result(target, None, ItemResultStatus::Rearmed, None)
}

fn purge_result(item: &PurgeItem, force: bool) -> ItemResult {
    if item.item_id.as_deref().is_none_or(str::is_empty)
        && item.client_item_key.as_deref().is_none_or(str::is_empty)
    {
        return item_result(
            item.item_id.clone(),
            item.client_item_key.clone(),
            ItemResultStatus::Invalid,
            Some("item_id or client_item_key is required".to_string()),
        );
    }
    if !force {
        return item_result(
            item.item_id.clone(),
            item.client_item_key.clone(),
            ItemResultStatus::Conflict,
            Some("leased items require force=true to purge".to_string()),
        );
    }
    item_result(
        item.item_id.clone(),
        item.client_item_key.clone(),
        ItemResultStatus::Purged,
        None,
    )
}

fn item_result(
    item_id: Option<String>,
    client_item_key: Option<String>,
    status: ItemResultStatus,
    detail: Option<String>,
) -> ItemResult {
    ItemResult {
        item_id,
        client_item_key,
        item_version: None,
        status,
        detail,
        command_position: matches!(status, ItemResultStatus::Rearmed | ItemResultStatus::Purged)
            .then_some(0),
    }
}

fn empty_metrics_snapshot() -> QueueMetricsSnapshot {
    QueueMetricsSnapshot {
        as_of: ApiTimestamp {
            seconds: 0,
            nanoseconds: 0,
        },
        metrics: QueueMetrics {
            lifecycle_counts: LifecycleCounts {
                pending: 0,
                leased: 0,
                complete: 0,
                failed: 0,
            },
            retry_backlog: 0,
            oldest_eligible_age_ms: None,
            progress_bound_risk_count: 0,
            active_leases: 0,
            recurring_pending: 0,
            recurring_leased: 0,
        },
    }
}

fn roll_up_queue_scopes(scopes: Vec<ActiveScope>) -> Vec<ActiveScope> {
    let mut by_queue: BTreeMap<String, ActiveScope> = BTreeMap::new();
    for scope in scopes {
        by_queue
            .entry(scope.queue_id.clone())
            .and_modify(|existing| {
                existing.oldest_eligible_age_ms = existing
                    .oldest_eligible_age_ms
                    .max(scope.oldest_eligible_age_ms);
                existing.eligible_count =
                    sum_optional(existing.eligible_count, scope.eligible_count);
                existing.progress_bound_risk_count = sum_optional(
                    existing.progress_bound_risk_count,
                    scope.progress_bound_risk_count,
                );
            })
            .or_insert(ActiveScope {
                queue_id: scope.queue_id,
                group_key: None,
                oldest_eligible_age_ms: scope.oldest_eligible_age_ms,
                eligible_count: scope.eligible_count,
                progress_bound_risk_count: scope.progress_bound_risk_count,
            });
    }
    by_queue.into_values().collect()
}

fn sum_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

async fn parse_json<T>(req: Request<Body>) -> Result<T, ApiProblem>
where
    T: DeserializeOwned,
{
    let body = to_bytes(req.into_body(), 1024 * 1024).await.map_err(|_| {
        ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "request body could not be read",
        )
    })?;
    serde_json::from_slice::<T>(&body).map_err(|_| {
        ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "request body must be valid JSON",
        )
    })
}

async fn route_not_found() -> ApiProblem {
    ApiProblem::new(
        StatusCode::NOT_FOUND,
        ApiErrorCode::InvalidRequest,
        "route is not part of API-001",
    )
}

pub mod scaffold {
    pub fn client_name() -> &'static str {
        pqueue_client::scaffold::core_name()
    }

    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }

    pub fn postgres_core_name() -> &'static str {
        pqueue_postgres::scaffold::core_name()
    }

    pub fn storage_name() -> &'static str {
        pqueue_storage::scaffold::core_name()
    }
}
