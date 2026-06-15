#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use pqueue_client::{
    ApiErrorCode, BatchClaimRequest, BatchClaimResponse, ClaimCompatibility, ClaimUnit,
    GateShardStatus, NativeRoute, ProblemDetails, SetGatesRequest, SetGatesResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

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
}

#[derive(Debug, Clone)]
pub struct AppState {
    auth: AuthContext,
    queue_catalog: QueueCatalog,
}

impl AppState {
    pub fn new(auth: AuthContext) -> Self {
        Self {
            auth,
            queue_catalog: QueueCatalog::default(),
        }
    }

    pub fn with_queue_catalog(auth: AuthContext, queue_catalog: QueueCatalog) -> Self {
        Self {
            auth,
            queue_catalog,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCapabilities {
    pub group_co_residency: bool,
    pub max_eligible_group_size: Option<u32>,
    pub cohort_policy_enabled: bool,
    pub cohort_completion_bound_ms: Option<u64>,
    pub progress_bound_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueCatalog {
    queues: BTreeMap<(String, String), QueueCapabilities>,
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

    pub fn capabilities(&self, tenant_id: &str, queue_id: &str) -> QueueCapabilities {
        self.queues
            .get(&(tenant_id.to_string(), queue_id.to_string()))
            .copied()
            .unwrap_or_default()
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

    Ok(Json(BatchClaimResponse {
        request_id: body.request_id,
        claim_unit,
        items: vec![],
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
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::PurgeItems,
        tenant_id,
        Some(queue_id),
        req,
        true,
    )
    .await
}

async fn renew_leases(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::RenewLeases,
        tenant_id,
        Some(queue_id),
        req,
        true,
    )
    .await
}

async fn batch_finalize(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::BatchFinalize,
        tenant_id,
        Some(queue_id),
        req,
        true,
    )
    .await
}

async fn get_queue_metrics(
    State(state): State<AppState>,
    Path((tenant_id, queue_id)): Path<(String, String)>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::GetQueueMetrics,
        tenant_id,
        Some(queue_id),
        req,
        false,
    )
    .await
}

async fn discover_active_scopes(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    req: Request<Body>,
) -> Result<Json<RouteStubResponse>, ApiProblem> {
    route_stub(
        state,
        NativeRoute::DiscoverActiveScopes,
        tenant_id,
        None,
        req,
        true,
    )
    .await
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
    if req.request_id.trim().is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "request_id is required",
        ));
    }
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
    if req.request_id.trim().is_empty() {
        return Err(ApiProblem::new(
            StatusCode::BAD_REQUEST,
            ApiErrorCode::InvalidRequest,
            "request_id is required",
        ));
    }
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
