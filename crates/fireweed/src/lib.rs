#![forbid(unsafe_code)]
//! # Fireweed
//!
//! Fireweed's ergonomic Rust embedding interface. Storage authority and projection choices are supplied
//! only to the `open_*` construction functions and are erased behind one concrete [`Fireweed`] handle.
//! The crate adds
//! ergonomic verbs over them: `create_queue` / `push` / `push_batch` / `upsert` / `claim` / `complete` /
//! `retry` / `release` / `fail` / `renew` / `reassign` / `rearm` / `purge` / `peek` / `claimed` /
//! `discover` / `metrics` — the full worker + operator surface, each composing a single pre-validating
//! engine port. A conceptual worker loop claims a batch, processes its items, then calls
//! [`Fireweed::complete`], [`Fireweed::retry`], or [`Fireweed::release`] with the resulting batch of
//! item ids:
//!
//! ```no_run
//! # use fireweed::{EngineResult, Fireweed, QueueKey};
//! # async fn worker(queue: &Fireweed, key: &QueueKey) -> EngineResult<()> {
//! loop {
//!     let claimed = queue.claim(key, 32, 30_000).await?;
//!     queue.complete(key, claimed.into_iter().map(|item| item.item_id)).await?;
//! }
//! # }
//! ```
//!
//! Lifecycle helpers remain batch-shaped even though they accept iterators. One call has the same
//! all-or-nothing failure behavior as [`Fireweed::ack`] and [`Fireweed::nack`]: a fenced, superseded, or
//! non-leased member rejects the call with its structured [`EngineError`] and commits none of that batch.
//! The older `ack`/`nack`/`discover_active_scopes` vocabulary remains supported without deprecation.
//!
//! Callers depend only on this crate and never inject, name, downcast, or recover a storage backend.
//! Errors use the structured [`EngineError`] contract.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(any(feature = "sqlite", feature = "objectlog", feature = "postgres", test))]
mod blocking_backend;
mod facade;

use axon_esf::encode_index_value;
// Internal-only types (not named in the public API surface).
pub use facade::{
    Fireweed, ProjectionControl, ProjectionControlCapabilities, ProjectionRebuild,
    ProjectionVerification,
};
use fireweed_engine::{
    Backend, BatchUpdatePort, ClaimPort, ClaimRequest, CommitEntryOutcome, CommitTransition,
    CommitTransitionEntry, CommitTransitionPort, ControlPlaneStore, DiscoveryPort, FinalizeOutcome,
    FinalizePort, HistoricalProjectionRead, HotProjectionQueryPort, IndexQueryPort,
    ItemMutationPort, LeaseState, OwnedSession, OwnershipOutcome, ProjectionRead, PurgePort,
    PushPort, PushSpec, QueueControlPlane, ReassignLeasePort, ReclaimPort, RecoveryReadPort,
    RenewLeasePort, ReschedulePort, SetGatesCommand, SetGatesPort, UpdateFieldsPort, UpsertPort,
    acquire_and_fence, validate_api001_reserved_write_fields, validate_claim_compatibility,
};

// ---------------------------------------------------------------------------
// PUBLIC DEPENDENCY SURFACE (ADR-009): a consumer depends on `fireweed` alone and can name every type its
// calls need — no direct dependency on `fireweed-core` / `fireweed-engine` required. Everything that appears in
// the public `Fireweed` interface is re-exported here.
// ---------------------------------------------------------------------------
pub use bytes::Bytes;
pub use fireweed_core::{
    AggregateGroup, BoundedMutationRequest, BoundedMutationResponse, BucketCount, BucketRule,
    ClaimByQueryRequest, ClientItemKey, CohortId, CohortOnIncomplete, CohortPolicy,
    CompoundIndexDef, CompoundIndexField, CreateQueue, CreateQueueError, CreateQueueErrorKind,
    DecimalValue, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, EligibilityPolicy,
    EntitySchemaDocument, FilterOp, GateKeyPolicy, GroupByField, GroupKey, GroupedAggregateRequest,
    GroupedAggregateResponse, IdentifierError, IndexDeclaration, IndexDef, IndexSpec, IndexType,
    ItemId, ItemState, LeaseToken, Metadata, MetadataValue, MetricsByQueryRequest, MutationOutcome,
    MutationResult, OrderField, OrderingMode, OwnerId, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueryCapabilityFlags, QueryCursor,
    QueryFilter, QueryRequestError, QueueCreationPolicy, QueueDefinition, QueueId, QueueIndex,
    RangeScanRequest, RangeScanResponse, RangeScanRow, RecurrenceMode, RecurrencePolicy, RequestId,
    RetryPolicy, SortDirection, TenantId, TimeBucket, TimestampError, TypedValue, UtcTimestamp,
    WorkerId,
};
pub use fireweed_engine::{
    ActiveScope, AddressedMutation, BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateOutcome,
    BatchUpdateRequest, BatchUpdateResponse, BatchUpdateValue, ClaimCompatibility, ClaimRef,
    Claimed, ClaimedItem, Clock, CommandPosition, CommitCapabilities, CommitEntryStatus,
    CommitRecovery, ControlPlaneConfig, CreateQueueOutcome, DiscoveryGranularity, EngineError,
    EngineResult, EntityEdit, EntityEditOperation, EntityPredicateValue, EntryRecovery,
    FinalizeKind, GateChange, GateKeyDelta, GroupBatching, IndexHit, InstanceFence,
    ItemMutationOperation, ItemMutationOutcome, ItemMutationPrecondition, ItemMutationRequest,
    ItemMutationResponse, ItemMutationResult, ItemMutationReturning, ItemMutationSelectorAggregate,
    ItemMutationSnapshot, ItemMutationSummary, ItemPatch, ItemPredicate, ItemSelector,
    ItemSelectorScope, ItemView, LeaseGuard, LifecyclePatch, LiveItemView, PayloadUpdate,
    PushBatchOutcome, PushDisposition, QueueKey, QueueMetrics, ScheduleUpdate, SelectedMutation,
    SideRecord, TimestampComparison, UpsertOutcome,
};

/// An active-scope result stamped with the exact queue and granularity used for discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveScopeDiscovery {
    pub queue: QueueKey,
    pub granularity: DiscoveryGranularity,
    pub scopes: Vec<ActiveScope>,
}

// Generic backend and legacy-composition tests execute inside the library crate so they retain access
// to crate-private implementation seams without publishing those seams to downstream crates. Cargo's
// implicit integration-test discovery is disabled; the true downstream facade tests remain explicit
// `[[test]]` targets in Cargo.toml.
#[cfg(test)]
extern crate self as fireweed;

#[cfg(test)]
#[path = "../tests/active_scope_routing.rs"]
mod test_active_scope_routing;
#[cfg(test)]
#[path = "../tests/coordination.rs"]
mod test_coordination;
#[cfg(test)]
#[path = "../tests/encapsulation.rs"]
mod test_encapsulation;
#[cfg(test)]
#[path = "../tests/facade.rs"]
mod test_facade;
#[cfg(test)]
#[path = "../tests/hot_projection_queries.rs"]
mod test_hot_projection_queries;
#[cfg(test)]
#[path = "../tests/multi_queue_claim.rs"]
mod test_multi_queue_claim;
#[cfg(test)]
#[path = "../tests/objectlog_postgres_composition.rs"]
mod test_objectlog_postgres_composition;
#[cfg(test)]
#[path = "../tests/objectlog_sqlite_composition.rs"]
mod test_objectlog_sqlite_composition;
#[cfg(test)]
#[path = "../tests/postgres_constructors.rs"]
mod test_postgres_constructors;
#[cfg(test)]
#[path = "../tests/product_validation_tests.rs"]
mod test_product_validation;
#[cfg(test)]
#[path = "../tests/queue_template.rs"]
mod test_queue_template;
#[cfg(test)]
#[path = "../tests/request_id_idempotency.rs"]
mod test_request_id_idempotency;
#[cfg(test)]
#[path = "../tests/schema_validation.rs"]
mod test_schema_validation;
#[cfg(test)]
#[path = "../tests/secondary_indexes.rs"]
mod test_secondary_indexes;
#[cfg(test)]
#[path = "../tests/vectorized_commit.rs"]
mod test_vectorized_commit;

/// A caller-attested, unfiltered leading prefix of one queue's group-granularity active-scope
/// discovery. Attestation validates the facts carried by [`ActiveScopeDiscovery`]—the granularity,
/// queue identity, non-empty input, and oldest-first age order—but cannot prove that the caller did
/// not filter or skip a leading scope before constructing the stamped input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OldestFirstScopePrefix {
    discovery: ActiveScopeDiscovery,
}

impl OldestFirstScopePrefix {
    /// Attest that `discovery` is an unfiltered leading prefix of the unchanged result returned by
    /// [`Fireweed::discover_active_scopes_stamped`]. The caller owns the unfiltered-prefix assertion;
    /// this method validates every property that can be checked locally.
    pub fn attest(discovery: ActiveScopeDiscovery) -> EngineResult<Self> {
        if discovery.granularity != DiscoveryGranularity::Group {
            return Err(EngineError::Invalid(
                "active-scope dispersion requires Group granularity",
            ));
        }
        if discovery.scopes.is_empty() {
            return Err(EngineError::Invalid(
                "active-scope prefix must not be empty",
            ));
        }
        if discovery
            .scopes
            .iter()
            .any(|scope| scope.queue_id != discovery.queue.queue_id.as_str())
        {
            return Err(EngineError::Invalid(
                "active-scope prefix queue_id does not match its discovery stamp",
            ));
        }
        if discovery
            .scopes
            .windows(2)
            .any(|pair| pair[0].oldest_eligible_age_ms < pair[1].oldest_eligible_age_ms)
        {
            return Err(EngineError::Invalid(
                "active-scope prefix must be ordered oldest eligible first",
            ));
        }
        Ok(Self { discovery })
    }

    /// Exact queue coordinate stamped by discovery.
    pub fn queue(&self) -> &QueueKey {
        &self.discovery.queue
    }

    /// Unchanged source-order descriptors covered by the caller's leading-prefix attestation.
    pub fn scopes(&self) -> &[ActiveScope] {
        &self.discovery.scopes
    }
}

/// Advisory selection from an [`OldestFirstScopePrefix`]. `scope` is borrowed directly from the
/// unchanged source prefix; selection does not reorder or synthesize descriptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveScopeSelection<'a> {
    pub index: usize,
    pub scope: &'a ActiveScope,
    /// Whether `scope.group_key` can be passed as an exact group claim filter. Ungrouped work remains
    /// selectable, but requires an ordinary unfiltered claim or another supported filter.
    pub group_filter_available: bool,
    /// Whether the oldest source scope was forced by the queue's progress-bound urgency rule.
    pub urgency_forced: bool,
}

/// Select one advisory group scope without changing backend discovery order.
///
/// Selection is deterministic for the same prefix and routing coordinates. Away from urgency it uses
/// SHA-256 rendezvous scores, length-framed over routing key, tenant, queue, and optional group identity,
/// and considers only the leading `candidate_window`. When the oldest scope's observed age plus stale-
/// input skew and urgency guard reaches `progress_bound_ms`, source index zero always wins. The helper
/// owns no scheduler state and makes no fairness or progress promise if callers stop polling, supply a
/// filtered/non-leading prefix, or ignore the advisory result.
pub fn select_active_scope_from_prefix<'a>(
    prefix: &'a OldestFirstScopePrefix,
    queue: &QueueKey,
    routing_key: &[u8],
    candidate_window: usize,
    progress_bound_ms: u64,
    observed_age_skew_ms: u64,
    urgency_guard_ms: u64,
) -> EngineResult<ActiveScopeSelection<'a>> {
    if prefix.queue() != queue {
        return Err(EngineError::Invalid(
            "active-scope prefix queue does not match selector queue",
        ));
    }
    if candidate_window == 0 {
        return Err(EngineError::Invalid(
            "active-scope candidate_window must be greater than zero",
        ));
    }
    if progress_bound_ms == 0 {
        return Err(EngineError::Invalid(
            "active-scope progress_bound_ms must be greater than zero",
        ));
    }
    if prefix
        .scopes()
        .iter()
        .any(|scope| scope.queue_id != queue.queue_id.as_str())
    {
        return Err(EngineError::Invalid(
            "active-scope prefix queue_id does not match selector queue",
        ));
    }

    let oldest = &prefix.scopes()[0];
    let urgency_forced = oldest
        .oldest_eligible_age_ms
        .saturating_add(observed_age_skew_ms)
        .saturating_add(urgency_guard_ms)
        >= progress_bound_ms;
    let index = if urgency_forced {
        0
    } else {
        let window = candidate_window.min(prefix.scopes().len());
        prefix.scopes()[..window]
            .iter()
            .enumerate()
            .max_by_key(|(_, scope)| active_scope_routing_score(routing_key, queue, scope))
            .map(|(index, _)| index)
            .expect("attested prefixes are non-empty")
    };
    let scope = &prefix.scopes()[index];
    Ok(ActiveScopeSelection {
        index,
        scope,
        group_filter_available: scope.group_key.is_some(),
        urgency_forced,
    })
}

fn active_scope_routing_score(
    routing_key: &[u8],
    queue: &QueueKey,
    scope: &ActiveScope,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    fn frame(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    let mut hasher = Sha256::new();
    hasher.update(b"fireweed-active-scope-dispersion-v1");
    frame(&mut hasher, routing_key);
    frame(&mut hasher, queue.tenant_id.as_str().as_bytes());
    frame(&mut hasher, queue.queue_id.as_str().as_bytes());
    match scope.group_key.as_deref() {
        Some(group) => {
            hasher.update([1]);
            frame(&mut hasher, group.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod active_scope_selector_unit_tests {
    use super::*;

    fn queue(tenant: &str, queue: &str) -> QueueKey {
        QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
    }

    fn scope(queue: &str, group: Option<&str>) -> ActiveScope {
        ActiveScope {
            queue_id: queue.to_string(),
            group_key: group.map(str::to_string),
            oldest_eligible_age_ms: 1,
            eligible_count: Some(1),
            progress_bound_risk_count: Some(0),
        }
    }

    #[test]
    fn routing_score_length_frames_every_identity_component() {
        let abc = queue("a", "bc");
        let ab_c = queue("ab", "c");
        assert_ne!(
            active_scope_routing_score(b"route", &abc, &scope("bc", Some("d"))),
            active_scope_routing_score(b"route", &ab_c, &scope("c", Some("d")))
        );

        let c_d = queue("c", "d");
        let bc_d = queue("bc", "d");
        assert_ne!(
            active_scope_routing_score(b"ab", &c_d, &scope("d", Some("e"))),
            active_scope_routing_score(b"a", &bc_d, &scope("d", Some("e")))
        );
        assert_ne!(
            active_scope_routing_score(b"route", &abc, &scope("bc", None)),
            active_scope_routing_score(b"route", &abc, &scope("bc", Some("")))
        );
        assert_ne!(
            active_scope_routing_score(b"route", &abc, &scope("bc", Some("d"))),
            active_scope_routing_score(b"route", &abc, &scope("bc", Some("de")))
        );
    }
}

/// Caller-owned, non-durable queue configuration that can be resolved for any [`QueueKey`].
///
/// The prototype's tenant and queue identifiers are deliberately discarded. The pinned creation policy
/// is applied by [`QueueTemplate::resolve`] before any backend call, while the optional name and revision
/// are diagnostics only and do not participate in template equality.
///
/// ```no_run
/// use std::sync::Arc;
/// use fireweed::{
///     CreateQueue, QueueCreationPolicy, QueueId, QueueKey, QueueTemplate, SystemClock, TenantId,
/// };
/// # fn prototype() -> CreateQueue { unimplemented!() }
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let template = QueueTemplate::new(prototype(), QueueCreationPolicy::default())
///     .with_name("email-jobs")
///     .with_revision("v2");
/// let queue = QueueKey::new(TenantId::new("acme")?, QueueId::new("outbound")?);
/// let fireweed = fireweed::open_memory(Arc::new(SystemClock));
/// let ensured = fireweed.ensure_queue(&queue, &template).await?;
/// assert_eq!(ensured.definition.queue_id, queue.queue_id);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct QueueTemplate {
    specification: KeylessQueueSpecification,
    policy: QueueCreationPolicy,
    template_name: Option<String>,
    template_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct KeylessQueueSpecification {
    priority_model: PriorityModel,
    ordering_mode: OrderingMode,
    max_rank_error: u32,
    progress_bound_ms: u64,
    eligibility_policy: EligibilityPolicy,
    cohort_policy: CohortPolicy,
    recurrence: RecurrencePolicy,
    request_id_retention_ms: u64,
    client_item_key_retention_ms: u64,
    terminal_retention_ms: u64,
    max_lease_duration_ms: u64,
    retry_policy: RetryPolicy,
    max_push_batch_size: u64,
    max_claim_batch_size: u64,
    max_eligible_group_size: Option<u64>,
    secondary_indexes: Vec<IndexSpec>,
    entity_schema: Option<EntitySchemaDocument>,
    typed_indexes: Vec<QueueIndex>,
    emit_change_records: bool,
}

impl QueueTemplate {
    /// Build a template from a complete create request and a caller-pinned creation policy.
    ///
    /// The prototype identifiers are ignored; [`Self::resolve`] always injects the supplied key.
    pub fn new(prototype: CreateQueue, policy: QueueCreationPolicy) -> Self {
        let CreateQueue {
            tenant_id: _,
            queue_id: _,
            priority_model,
            ordering_mode,
            max_rank_error,
            progress_bound_ms,
            eligibility_policy,
            cohort_policy,
            recurrence,
            request_id_retention_ms,
            client_item_key_retention_ms,
            terminal_retention_ms,
            max_lease_duration_ms,
            retry_policy,
            max_push_batch_size,
            max_claim_batch_size,
            max_eligible_group_size,
            secondary_indexes,
            entity_schema,
            typed_indexes,
            emit_change_records,
        } = prototype;
        Self {
            specification: KeylessQueueSpecification {
                priority_model,
                ordering_mode,
                max_rank_error,
                progress_bound_ms,
                eligibility_policy,
                cohort_policy,
                recurrence,
                request_id_retention_ms,
                client_item_key_retention_ms,
                terminal_retention_ms,
                max_lease_duration_ms,
                retry_policy,
                max_push_batch_size,
                max_claim_batch_size,
                max_eligible_group_size,
                secondary_indexes,
                entity_schema,
                typed_indexes,
                emit_change_records,
            },
            policy,
            template_name: None,
            template_revision: None,
        }
    }

    /// Attach a non-durable diagnostic name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.template_name = Some(name.into());
        self
    }

    /// Attach a non-durable diagnostic revision.
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.template_revision = Some(revision.into());
        self
    }

    /// Resolve this template for `key`, validating with the pinned creation policy.
    pub fn resolve(&self, key: &QueueKey) -> Result<QueueDefinition, CreateQueueError> {
        let KeylessQueueSpecification {
            priority_model,
            ordering_mode,
            max_rank_error,
            progress_bound_ms,
            eligibility_policy,
            cohort_policy,
            recurrence,
            request_id_retention_ms,
            client_item_key_retention_ms,
            terminal_retention_ms,
            max_lease_duration_ms,
            retry_policy,
            max_push_batch_size,
            max_claim_batch_size,
            max_eligible_group_size,
            secondary_indexes,
            entity_schema,
            typed_indexes,
            emit_change_records,
        } = self.specification.clone();
        CreateQueue {
            tenant_id: key.tenant_id.clone(),
            queue_id: key.queue_id.clone(),
            priority_model,
            ordering_mode,
            max_rank_error,
            progress_bound_ms,
            eligibility_policy,
            cohort_policy,
            recurrence,
            request_id_retention_ms,
            client_item_key_retention_ms,
            terminal_retention_ms,
            max_lease_duration_ms,
            retry_policy,
            max_push_batch_size,
            max_claim_batch_size,
            max_eligible_group_size,
            secondary_indexes,
            entity_schema,
            typed_indexes,
            emit_change_records,
        }
        .validate(&self.policy)
    }
}

impl PartialEq for QueueTemplate {
    fn eq(&self, other: &Self) -> bool {
        self.specification == other.specification && self.policy == other.policy
    }
}

/// Successful result of [`Fireweed::ensure_queue`]. Template diagnostics are not persisted.
#[derive(Debug, Clone, PartialEq)]
pub struct EnsureQueueOutcome {
    pub created: bool,
    pub definition: QueueDefinition,
    pub template_name: Option<String>,
    pub template_revision: Option<String>,
}

/// Typed, façade-local failure from [`Fireweed::ensure_queue`].
#[derive(Debug, Clone, PartialEq)]
pub enum EnsureQueueError {
    Validation {
        error: CreateQueueError,
        template_name: Option<String>,
        template_revision: Option<String>,
    },
    Backend {
        error: EngineError,
        template_name: Option<String>,
        template_revision: Option<String>,
    },
    DefinitionConflict {
        created: bool,
        desired: Box<QueueDefinition>,
        stored: Box<QueueDefinition>,
        template_name: Option<String>,
        template_revision: Option<String>,
    },
}

impl fmt::Display for EnsureQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation { error, .. } => {
                write!(f, "queue template validation failed: {error}")
            }
            Self::Backend { error, .. } => write!(f, "queue ensure backend failed: {error}"),
            Self::DefinitionConflict { .. } => f.write_str("queue definition conflict"),
        }
    }
}

impl std::error::Error for EnsureQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Validation { error, .. } => Some(error),
            Self::Backend { error, .. } => Some(error),
            Self::DefinitionConflict { .. } => None,
        }
    }
}

/// Wall-clock [`Clock`] for production use — pass `Arc::new(SystemClock)` to any `open_*` constructor.
/// Tests inject a controllable clock instead (e.g. `fireweed_memory::ManualClock`). Provided here so a
/// consumer depending on `fireweed` alone has a ready clock without naming `fireweed-engine`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}

/// An owned secret used by composed storage configuration. Its value is redacted from `Debug` and has
/// no public accessor; the composition root consumes it internally.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SecretValue(String);

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

/// Authoritative command-log storage selected by an composed deployment.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum ObjectLogConfig {
    Local {
        root: PathBuf,
    },
    S3Compatible {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        allow_insecure_http: bool,
    },
}

impl fmt::Debug for ObjectLogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local { root } => f.debug_struct("Local").field("root", root).finish(),
            Self::S3Compatible {
                endpoint,
                bucket,
                region,
                allow_insecure_http,
                ..
            } => f
                .debug_struct("S3Compatible")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("region", region)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field("allow_insecure_http", allow_insecure_http)
                .finish(),
        }
    }
}

/// Disposable materialized projection selected by an composed object-log deployment
/// (crate-private; the public projection axis is [`ProjectionStoreConfig`]).
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum ComposedProjectionConfig {
    Sqlite {
        path: PathBuf,
    },
    /// The URL may contain credentials and is therefore redacted from diagnostics.
    Postgres {
        url: SecretValue,
    },
}

impl fmt::Debug for ComposedProjectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite { path } => f.debug_struct("Sqlite").field("path", path).finish(),
            Self::Postgres { .. } => f
                .debug_struct("Postgres")
                .field("url", &"<redacted>")
                .finish(),
        }
    }
}

/// The acknowledgement barrier for composed object-log compositions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitResponseBarrier {
    /// Success requires both the authoritative manifest and durable projection.
    Strict,
    /// Success requires the authoritative manifest and hot projection; durable projection apply may lag.
    AsyncProjection,
}

/// Group-commit segment settings for the authoritative object log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentSettings {
    pub target_bytes: usize,
    pub max_latency_ms: u64,
}

impl SegmentSettings {
    pub fn new(target_bytes: usize, max_latency_ms: u64) -> EngineResult<Self> {
        if target_bytes == 0 || max_latency_ms == 0 {
            return Err(EngineError::Invalid(
                "object-log segment target and latency must be non-zero",
            ));
        }
        Ok(Self {
            target_bytes,
            max_latency_ms,
        })
    }
}

/// Action taken when a disposable projection is absent or incompatible with the authoritative log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionRecoveryAction {
    FailClosed,
    /// Delete only the disposable projection namespace and rebuild from authoritative history.
    RebuildProjection,
}

/// Recovery bounds and validation policy for an composed storage composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProjectionRecoveryPolicy {
    pub incompatible_projection: ProjectionRecoveryAction,
    pub verify_checksums: bool,
    pub max_tail_commands: u64,
}

impl Default for ProjectionRecoveryPolicy {
    fn default() -> Self {
        Self {
            incompatible_projection: ProjectionRecoveryAction::FailClosed,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        }
    }
}

#[cfg(any(feature = "objectlog", test))]
fn object_log_namespace(namespace: &str) -> String {
    namespace
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(all(feature = "objectlog", feature = "postgres"))]
use sha2::{Digest, Sha256};

#[cfg(all(feature = "objectlog", feature = "postgres"))]
fn derived_postgres_schema_name(namespace: &str) -> String {
    const PREFIX: &str = "fireweed_";
    const HASH_BYTES: usize = 27;

    let digest = Sha256::digest(namespace.as_bytes());
    let mut schema = String::with_capacity(PREFIX.len() + HASH_BYTES * 2);
    schema.push_str(PREFIX);
    for byte in digest.iter().take(HASH_BYTES) {
        schema.push_str(&format!("{byte:02x}"));
    }
    schema
}

/// Private normalized configuration for a composed authoritative-log plus
/// disposable-projection pair. Public callers use [`ObjectLogRuntimeConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ComposedStorageConfig {
    pub object_log: ObjectLogConfig,
    pub object_log_authority: ObjectLogAuthorityConfig,
    pub projection: ComposedProjectionConfig,
    pub response_barrier: CommitResponseBarrier,
    pub segments: SegmentSettings,
    pub namespace: String,
    pub recovery: ProjectionRecoveryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObjectLogAuthorityConfig {
    NativeConditionalWrite,
    Postgres { url: SecretValue },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConfigSecret(SecretValue);

impl ConfigSecret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretValue::new(value))
    }
}

impl fmt::Debug for ConfigSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfigSecret(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLogStorage {
    Local {
        root: PathBuf,
    },
    S3Compatible {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: ConfigSecret,
        secret_access_key: ConfigSecret,
        allow_insecure_http: bool,
    },
}

/// Linearization authority for object-log manifest publication.
///
/// S3-compatible stores that do not provide atomic conditional object creation must use the PostgreSQL
/// authority. Local filesystem storage uses its native conditional-create implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLogAuthority {
    NativeConditionalWrite,
    Postgres { url: ConfigSecret },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionConfig {
    Sqlite { path: PathBuf },
    Postgres { url: ConfigSecret },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseBarrier {
    Strict,
    AsyncProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentConfig {
    pub target_bytes: usize,
    pub max_latency_ms: u64,
}

impl SegmentConfig {
    pub fn new(target_bytes: usize, max_latency_ms: u64) -> EngineResult<Self> {
        SegmentSettings::new(target_bytes, max_latency_ms)?;
        Ok(Self {
            target_bytes,
            max_latency_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    FailClosed,
    RebuildProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPolicy {
    pub incompatible_projection: RecoveryAction,
    pub verify_checksums: bool,
    pub max_tail_commands: u64,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        let current = ProjectionRecoveryPolicy::default();
        Self {
            incompatible_projection: RecoveryAction::FailClosed,
            verify_checksums: current.verify_checksums,
            max_tail_commands: current.max_tail_commands,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLogRuntimeConfig {
    pub object_log: ObjectLogStorage,
    pub authority: ObjectLogAuthority,
    pub projection: ProjectionConfig,
    pub response_barrier: ResponseBarrier,
    pub segments: SegmentConfig,
    pub namespace: String,
    pub recovery: RecoveryPolicy,
}

/// PostgreSQL log-axis mode (API-005). Construction input only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresMode {
    LogReplay,
    Relational,
}

/// Multi-instance coordination inputs for a PostgreSQL composition (API-005).
#[derive(Debug, Clone)]
pub struct PostgresCoordinationConfig {
    pub instance_id: OwnerId,
    pub control_plane: ControlPlaneConfig,
}

/// Convenience construction inputs for `open_postgres_runtime*` (maps onto [`LogConfig::Postgres`]).
#[derive(Debug, Clone)]
pub struct PostgresRuntimeConfig {
    pub url: ConfigSecret,
    pub schema: Option<String>,
    pub mode: PostgresMode,
    pub node_id: Option<u8>,
    pub coordination: Option<PostgresCoordinationConfig>,
}

/// Public log axis: five first-class values (orthogonal storage matrix / API-005).
#[derive(Debug, Clone)]
pub enum LogConfig {
    /// Class B: in-process command log (no log rebuild after process death).
    Memory,
    /// Class A: durable SQLite command log.
    Sqlite { path: PathBuf },
    /// Class A: durable PostgreSQL command log.
    Postgres {
        url: ConfigSecret,
        schema: Option<String>,
        mode: PostgresMode,
        node_id: Option<u8>,
        coordination: Option<PostgresCoordinationConfig>,
    },
    /// Class A: local directory tree / NAS path object log (same protocol as S3).
    Filesystem { root: PathBuf },
    /// Class A: S3-compatible object log.
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: ConfigSecret,
        secret_access_key: ConfigSecret,
        allow_insecure_http: bool,
    },
}

impl LogConfig {
    /// Canonical public name for this log axis value.
    pub fn axis_name(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite { .. } => "sqlite",
            Self::Postgres { .. } => "postgres",
            Self::Filesystem { .. } => "filesystem",
            Self::S3 { .. } => "s3",
        }
    }

    /// Class A (durable log) vs Class B (memory log) durability envelope.
    pub fn is_durable_log(&self) -> bool {
        !matches!(self, Self::Memory)
    }
}

/// Public projection axis: three first-class values (orthogonal storage matrix / API-005).
///
/// Object-log convenience constructors still use [`ProjectionConfig`] (sqlite/postgres only);
/// full-matrix work uses this type (includes [`Memory`](Self::Memory)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionStoreConfig {
    Memory,
    Sqlite { path: PathBuf },
    Postgres { url: ConfigSecret },
}

impl ProjectionStoreConfig {
    /// Canonical public name for this projection axis value.
    pub fn axis_name(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite { .. } => "sqlite",
            Self::Postgres { .. } => "postgres",
        }
    }
}

/// Normative composition root for log × projection (+ related axes). API-005 / product brief.
///
/// Every cell of the 5×3 matrix is a valid selection; durability class differs by log axis
/// ([`LogConfig::is_durable_log`]). Opening a not-yet-wired cell may still return
/// [`EngineError::Unavailable`] until Phase 2 composition beads land — construction and
/// validation of this type accept all 15 pairs.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub log: LogConfig,
    pub projection: ProjectionStoreConfig,
    pub control_plane: Option<ControlPlaneConfig>,
    /// Object-log peers only ([`LogConfig::Filesystem`], [`LogConfig::S3`]); ignored for other logs.
    pub authority: Option<ObjectLogAuthority>,
    pub response_barrier: ResponseBarrier,
    pub segments: SegmentConfig,
    pub namespace: String,
    pub recovery: RecoveryPolicy,
}

impl StorageConfig {
    /// Class B reference cell: memory log × memory projection.
    pub fn memory() -> Self {
        Self {
            log: LogConfig::Memory,
            projection: ProjectionStoreConfig::Memory,
            control_plane: None,
            authority: None,
            response_barrier: ResponseBarrier::Strict,
            segments: SegmentConfig {
                target_bytes: 1024 * 1024,
                max_latency_ms: 5,
            },
            namespace: "default".to_owned(),
            recovery: RecoveryPolicy::default(),
        }
    }

    /// Map an object-log convenience config onto the full-matrix surface
    /// (`Local` → [`LogConfig::Filesystem`], `S3Compatible` → [`LogConfig::S3`]).
    pub fn from_object_log_runtime(config: ObjectLogRuntimeConfig) -> Self {
        config.into_matrix_config()
    }

    /// Structural validation for the 5×3 matrix. Does not open stores.
    ///
    /// Returns [`EngineError::Invalid`] for malformed fields and
    /// [`EngineError::Unavailable`] for clearly mismatched object-log authority /
    /// barrier combinations (API-005 intent).
    pub fn validate(&self) -> EngineResult<()> {
        if self.namespace.trim().is_empty() {
            return Err(EngineError::Invalid(
                "storage namespace must not be empty",
            ));
        }
        if self.segments.target_bytes == 0 || self.segments.max_latency_ms == 0 {
            return Err(EngineError::Invalid(
                "segment target_bytes and max_latency_ms must be non-zero",
            ));
        }
        if self.recovery.max_tail_commands == 0 {
            return Err(EngineError::Invalid(
                "recovery max_tail_commands must be non-zero",
            ));
        }

        match &self.log {
            LogConfig::Memory => {}
            LogConfig::Sqlite { path } if path.as_os_str().is_empty() => {
                return Err(EngineError::Invalid("sqlite log path must not be empty"));
            }
            LogConfig::Sqlite { .. } => {}
            LogConfig::Postgres { url, .. } if url.0.is_empty() => {
                return Err(EngineError::Invalid(
                    "postgres log URL must not be empty",
                ));
            }
            LogConfig::Postgres { .. } => {}
            LogConfig::Filesystem { root } if root.as_os_str().is_empty() => {
                return Err(EngineError::Invalid(
                    "filesystem object-log root must not be empty",
                ));
            }
            LogConfig::Filesystem { .. } => {
                match &self.authority {
                    None | Some(ObjectLogAuthority::NativeConditionalWrite) => {}
                    Some(ObjectLogAuthority::Postgres { .. }) => {
                        return Err(EngineError::Invalid(
                            "filesystem object-log requires NativeConditionalWrite authority",
                        ));
                    }
                }
            }
            LogConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                ..
            } => {
                if endpoint.is_empty()
                    || bucket.is_empty()
                    || region.is_empty()
                    || access_key_id.0.is_empty()
                    || secret_access_key.0.is_empty()
                {
                    return Err(EngineError::Invalid(
                        "S3 object-log configuration fields must not be empty",
                    ));
                }
                if let Some(ObjectLogAuthority::Postgres { url }) = &self.authority
                    && url.0.is_empty()
                {
                    return Err(EngineError::Invalid(
                        "PostgreSQL object-log authority URL must not be empty",
                    ));
                }
            }
        }

        match &self.projection {
            ProjectionStoreConfig::Memory => {}
            ProjectionStoreConfig::Sqlite { path } if path.as_os_str().is_empty() => {
                return Err(EngineError::Invalid(
                    "sqlite projection path must not be empty",
                ));
            }
            ProjectionStoreConfig::Sqlite { .. } => {}
            ProjectionStoreConfig::Postgres { url } if url.0.is_empty() => {
                return Err(EngineError::Invalid(
                    "postgres projection URL must not be empty",
                ));
            }
            ProjectionStoreConfig::Postgres { .. } => {
                // Object-log × Postgres projection requires a strict response barrier.
                if matches!(
                    &self.log,
                    LogConfig::Filesystem { .. } | LogConfig::S3 { .. }
                ) && self.response_barrier != ResponseBarrier::Strict
                {
                    return Err(EngineError::Unavailable);
                }
            }
        }

        Ok(())
    }
}

impl ObjectLogRuntimeConfig {
    /// Map this convenience config onto the normative full-matrix [`StorageConfig`].
    pub fn into_matrix_config(self) -> StorageConfig {
        StorageConfig {
            log: match self.object_log {
                ObjectLogStorage::Local { root } => LogConfig::Filesystem { root },
                ObjectLogStorage::S3Compatible {
                    endpoint,
                    bucket,
                    region,
                    access_key_id,
                    secret_access_key,
                    allow_insecure_http,
                } => LogConfig::S3 {
                    endpoint,
                    bucket,
                    region,
                    access_key_id,
                    secret_access_key,
                    allow_insecure_http,
                },
            },
            projection: match self.projection {
                ProjectionConfig::Sqlite { path } => ProjectionStoreConfig::Sqlite { path },
                ProjectionConfig::Postgres { url } => ProjectionStoreConfig::Postgres { url },
            },
            control_plane: None,
            authority: Some(self.authority),
            response_barrier: self.response_barrier,
            segments: self.segments,
            namespace: self.namespace,
            recovery: self.recovery,
        }
    }

    fn into_storage_config(self) -> ComposedStorageConfig {
        ComposedStorageConfig {
            object_log: match self.object_log {
                ObjectLogStorage::Local { root } => ObjectLogConfig::Local { root },
                ObjectLogStorage::S3Compatible {
                    endpoint,
                    bucket,
                    region,
                    access_key_id,
                    secret_access_key,
                    allow_insecure_http,
                } => ObjectLogConfig::S3Compatible {
                    endpoint,
                    bucket,
                    region,
                    access_key_id: access_key_id.0,
                    secret_access_key: secret_access_key.0,
                    allow_insecure_http,
                },
            },
            object_log_authority: match self.authority {
                ObjectLogAuthority::NativeConditionalWrite => {
                    ObjectLogAuthorityConfig::NativeConditionalWrite
                }
                ObjectLogAuthority::Postgres { url } => {
                    ObjectLogAuthorityConfig::Postgres { url: url.0 }
                }
            },
            projection: match self.projection {
                ProjectionConfig::Sqlite { path } => ComposedProjectionConfig::Sqlite { path },
                ProjectionConfig::Postgres { url } => {
                    ComposedProjectionConfig::Postgres { url: url.0 }
                }
            },
            response_barrier: match self.response_barrier {
                ResponseBarrier::Strict => CommitResponseBarrier::Strict,
                ResponseBarrier::AsyncProjection => CommitResponseBarrier::AsyncProjection,
            },
            segments: SegmentSettings {
                target_bytes: self.segments.target_bytes,
                max_latency_ms: self.segments.max_latency_ms,
            },
            namespace: self.namespace,
            recovery: ProjectionRecoveryPolicy {
                incompatible_projection: match self.recovery.incompatible_projection {
                    RecoveryAction::FailClosed => ProjectionRecoveryAction::FailClosed,
                    RecoveryAction::RebuildProjection => {
                        ProjectionRecoveryAction::RebuildProjection
                    }
                },
                verify_checksums: self.recovery.verify_checksums,
                max_tail_commands: self.recovery.max_tail_commands,
            },
        }
    }

    pub fn validate(&self) -> EngineResult<()> {
        // Full-matrix validation first (covers empty fields / authority pairing).
        self.clone().into_matrix_config().validate()?;
        self.clone().into_storage_config().validate()
    }
}

#[cfg(test)]
mod storage_config_matrix_tests {
    use super::*;
    use std::path::PathBuf;

    fn segments() -> SegmentConfig {
        SegmentConfig::new(1024, 5).expect("valid segments")
    }

    fn base(log: LogConfig, projection: ProjectionStoreConfig) -> StorageConfig {
        StorageConfig {
            log,
            projection,
            control_plane: None,
            authority: None,
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: "matrix-test".to_owned(),
            recovery: RecoveryPolicy::default(),
        }
    }

    fn all_logs() -> Vec<LogConfig> {
        vec![
            LogConfig::Memory,
            LogConfig::Sqlite {
                path: PathBuf::from("/tmp/log.db"),
            },
            LogConfig::Postgres {
                url: ConfigSecret::new("postgres://localhost/fireweed"),
                schema: Some("fw".to_owned()),
                mode: PostgresMode::Relational,
                node_id: Some(1),
                coordination: None,
            },
            LogConfig::Filesystem {
                root: PathBuf::from("/var/lib/fireweed/object-log"),
            },
            LogConfig::S3 {
                endpoint: "https://s3.example".to_owned(),
                bucket: "fireweed".to_owned(),
                region: "us-east-1".to_owned(),
                access_key_id: ConfigSecret::new("akid"),
                secret_access_key: ConfigSecret::new("secret"),
                allow_insecure_http: false,
            },
        ]
    }

    fn all_projections() -> Vec<ProjectionStoreConfig> {
        vec![
            ProjectionStoreConfig::Memory,
            ProjectionStoreConfig::Sqlite {
                path: PathBuf::from("/tmp/projection.db"),
            },
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new("postgres://localhost/projection"),
            },
        ]
    }

    #[test]
    fn constructs_and_validates_all_five_logs_and_three_projections() {
        let logs = all_logs();
        assert_eq!(logs.len(), 5);
        let projections = all_projections();
        assert_eq!(projections.len(), 3);

        let mut axis_names = Vec::new();
        for log in &logs {
            axis_names.push(log.axis_name());
            assert_eq!(log.is_durable_log(), log.axis_name() != "memory");
        }
        assert_eq!(
            axis_names,
            vec!["memory", "sqlite", "postgres", "filesystem", "s3"]
        );
        assert_eq!(
            projections
                .iter()
                .map(|p| p.axis_name())
                .collect::<Vec<_>>(),
            vec!["memory", "sqlite", "postgres"]
        );

        // All 15 matrix cells construct and structurally validate.
        let mut cells = 0usize;
        for log in logs {
            for projection in &projections {
                let mut config = base(log.clone(), projection.clone());
                if matches!(
                    &config.log,
                    LogConfig::Filesystem { .. } | LogConfig::S3 { .. }
                ) {
                    config.authority = Some(ObjectLogAuthority::NativeConditionalWrite);
                }
                config
                    .validate()
                    .unwrap_or_else(|e| panic!("cell {}×{}: {e:?}", config.log.axis_name(), projection.axis_name()));
                cells += 1;
            }
        }
        assert_eq!(cells, 15);
    }

    #[test]
    fn memory_helper_and_objectlog_mapping() {
        let mem = StorageConfig::memory();
        assert!(matches!(mem.log, LogConfig::Memory));
        assert!(matches!(mem.projection, ProjectionStoreConfig::Memory));
        mem.validate().expect("memory defaults validate");

        let ol = ObjectLogRuntimeConfig {
            object_log: ObjectLogStorage::Local {
                root: PathBuf::from("/data/log"),
            },
            authority: ObjectLogAuthority::NativeConditionalWrite,
            projection: ProjectionConfig::Sqlite {
                path: PathBuf::from("/data/proj.db"),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: "ol".to_owned(),
            recovery: RecoveryPolicy::default(),
        };
        let mapped = StorageConfig::from_object_log_runtime(ol);
        assert!(matches!(mapped.log, LogConfig::Filesystem { .. }));
        assert!(matches!(mapped.projection, ProjectionStoreConfig::Sqlite { .. }));
        mapped.validate().expect("mapped object-log config validates");
    }

    #[test]
    fn rejects_empty_paths_and_filesystem_postgres_authority() {
        let mut bad = StorageConfig::memory();
        bad.log = LogConfig::Sqlite {
            path: PathBuf::new(),
        };
        assert!(matches!(bad.validate(), Err(EngineError::Invalid(_))));

        let mut fs = base(
            LogConfig::Filesystem {
                root: PathBuf::from("/tank/log"),
            },
            ProjectionStoreConfig::Memory,
        );
        fs.authority = Some(ObjectLogAuthority::Postgres {
            url: ConfigSecret::new("postgres://authority"),
        });
        assert!(matches!(fs.validate(), Err(EngineError::Invalid(_))));
    }
}

impl ComposedStorageConfig {
    pub fn validate(&self) -> EngineResult<()> {
        if self.namespace.trim().is_empty() {
            return Err(EngineError::Invalid(
                "object-log runtime namespace must not be empty",
            ));
        }
        match &self.object_log {
            ObjectLogConfig::Local { root } if root.as_os_str().is_empty() => {
                return Err(EngineError::Invalid(
                    "local object-log root must not be empty",
                ));
            }
            ObjectLogConfig::S3Compatible {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                ..
            } if endpoint.is_empty()
                || bucket.is_empty()
                || region.is_empty()
                || access_key_id.is_empty()
                || secret_access_key.is_empty() =>
            {
                return Err(EngineError::Invalid(
                    "S3-compatible object-log configuration fields must not be empty",
                ));
            }
            _ => {}
        }
        match (&self.object_log, &self.object_log_authority) {
            (ObjectLogConfig::Local { .. }, ObjectLogAuthorityConfig::NativeConditionalWrite) => {}
            (ObjectLogConfig::Local { .. }, ObjectLogAuthorityConfig::Postgres { .. }) => {
                return Err(EngineError::Invalid(
                    "local object-log storage requires native conditional-write authority",
                ));
            }
            (
                ObjectLogConfig::S3Compatible { .. },
                ObjectLogAuthorityConfig::NativeConditionalWrite,
            ) => {}
            (ObjectLogConfig::S3Compatible { .. }, ObjectLogAuthorityConfig::Postgres { url })
                if url.is_empty() =>
            {
                return Err(EngineError::Invalid(
                    "PostgreSQL object-log authority URL must not be empty",
                ));
            }
            (ObjectLogConfig::S3Compatible { .. }, ObjectLogAuthorityConfig::Postgres { .. }) => {}
        }
        match &self.projection {
            ComposedProjectionConfig::Sqlite { path } if path.as_os_str().is_empty() => {
                return Err(EngineError::Invalid(
                    "SQLite projection path must not be empty",
                ));
            }
            ComposedProjectionConfig::Postgres { url } if url.is_empty() => {
                return Err(EngineError::Invalid(
                    "PostgreSQL projection URL must not be empty",
                ));
            }
            _ => {}
        }
        if self.recovery.max_tail_commands == 0 {
            return Err(EngineError::Invalid(
                "object-log recovery tail bound must be non-zero",
            ));
        }
        Ok(())
    }
}

/// Projection lifecycle operations supported by an [`ProjectionLifecycleHandle`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProjectionLifecycleCapabilities {
    pub verify_projection: bool,
    pub delete_projection: bool,
    pub rebuild_projection: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProjectionVerificationState {
    pub compatible: bool,
    pub projection_sequence: u64,
    pub authoritative_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProjectionRebuildState {
    pub snapshot_used: bool,
    pub tail_commands_replayed: u64,
    pub projection_sequence: u64,
}

struct ProjectionLifecycleHandleInner {
    _config: ComposedStorageConfig,
    lifecycle: Box<dyn ProjectionLifecycle>,
}

type ProjectionLifecycleFuture<'a, T> = Pin<Box<dyn Future<Output = EngineResult<T>> + Send + 'a>>;

trait ProjectionLifecycle: Send + Sync {
    fn capabilities(&self) -> ProjectionLifecycleCapabilities;
    #[allow(dead_code)] // Exercised by crate-internal lifecycle tests for async projection flushing.
    fn buffered_group_commit_commands(&self) -> Option<usize> {
        None
    }
    fn verify_projection(&self) -> ProjectionLifecycleFuture<'_, ProjectionVerificationState>;
    fn delete_projection(&self) -> ProjectionLifecycleFuture<'_, ()>;
    fn rebuild_projection(&self) -> ProjectionLifecycleFuture<'_, ProjectionRebuildState>;
    fn shutdown(&mut self);
}

impl Drop for ProjectionLifecycleHandleInner {
    fn drop(&mut self) {
        self.lifecycle.shutdown();
    }
}

/// Opaque, cloneable ownership boundary for composed storage lifecycle state.
///
/// The handle owns its configuration and, when a concrete composition is installed, its background
/// flusher/checkpoint lifecycle. Dropping the last clone is the shutdown boundary. No concrete adapter
/// type appears in the public signature.
#[derive(Clone)]
pub(crate) struct ProjectionLifecycleHandle {
    inner: Arc<ProjectionLifecycleHandleInner>,
}

impl fmt::Debug for ProjectionLifecycleHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionLifecycleHandle")
            .field("capabilities", &self.lifecycle_capabilities())
            .finish_non_exhaustive()
    }
}

impl ProjectionLifecycleHandle {
    pub fn lifecycle_capabilities(&self) -> ProjectionLifecycleCapabilities {
        self.inner.lifecycle.capabilities()
    }

    /// Number of accepted commands waiting for a group-commit seal, when the composed storage
    /// exposes that observation. This is a diagnostic synchronization seam, not a durability barrier;
    /// lifecycle operations remain responsible for quiescing accepted writes.
    #[doc(hidden)]
    #[allow(dead_code)] // Retained for crate-internal lifecycle tests.
    pub fn buffered_group_commit_commands(&self) -> Option<usize> {
        self.inner.lifecycle.buffered_group_commit_commands()
    }

    pub async fn verify_projection(&self) -> EngineResult<ProjectionVerificationState> {
        if !self.lifecycle_capabilities().verify_projection {
            return Err(EngineError::Unavailable);
        }
        self.inner.lifecycle.verify_projection().await
    }

    pub async fn delete_projection(&self) -> EngineResult<()> {
        if !self.lifecycle_capabilities().delete_projection {
            return Err(EngineError::Unavailable);
        }
        self.inner.lifecycle.delete_projection().await
    }

    pub async fn rebuild_projection(&self) -> EngineResult<ProjectionRebuildState> {
        if !self.lifecycle_capabilities().rebuild_projection {
            return Err(EngineError::Unavailable);
        }
        self.inner.lifecycle.rebuild_projection().await
    }
}

/// A composed runtime paired with its opaque durability lifecycle handle.
///
/// Private composition helper that pairs the internal runtime with its projection lifecycle handle.
#[doc(hidden)]
pub(crate) struct ComposedRuntime<B> {
    runtime: RuntimeCore<B>,
    lifecycle: ProjectionLifecycleHandle,
}

#[allow(dead_code)] // Private wrapper retained solely for internal regression coverage.
impl<B> ComposedRuntime<B> {
    pub fn lifecycle_capabilities(&self) -> ProjectionLifecycleCapabilities {
        self.lifecycle.lifecycle_capabilities()
    }

    /// See [`ProjectionLifecycleHandle::buffered_group_commit_commands`].
    #[doc(hidden)]
    pub fn buffered_group_commit_commands(&self) -> Option<usize> {
        self.lifecycle.buffered_group_commit_commands()
    }

    pub async fn verify_projection(&self) -> EngineResult<ProjectionVerificationState> {
        self.lifecycle.verify_projection().await
    }

    pub async fn delete_projection(&self) -> EngineResult<()> {
        self.lifecycle.delete_projection().await
    }

    pub async fn rebuild_projection(&self) -> EngineResult<ProjectionRebuildState> {
        self.lifecycle.rebuild_projection().await
    }

    pub fn lifecycle_handle(&self) -> ProjectionLifecycleHandle {
        self.lifecycle.clone()
    }

    fn into_fireweed(self) -> Fireweed
    where
        B: LibBackend + BatchUpdatePort + ItemMutationPort + 'static,
    {
        Fireweed::from_runtime_with_projection(self.runtime, self.lifecycle)
    }
}

impl<B> Deref for ComposedRuntime<B> {
    type Target = RuntimeCore<B>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

#[cfg(all(feature = "objectlog", feature = "postgres"))]
type ObjectLogPostgresBackend = fireweed_engine::ComposedBackend<
    fireweed_objectlog::ObjectLog,
    fireweed_postgres::PostgresRelational,
    fireweed_engine::InProcessControlPlane,
>;

#[cfg(all(feature = "objectlog", feature = "postgres"))]
struct ObjectLogPostgresLifecycle {
    backend: Option<Arc<ObjectLogPostgresBackend>>,
    executor: blocking_backend::OwnedBlockingExecutor,
    max_tail_commands: u64,
    stop: Arc<std::sync::atomic::AtomicBool>,
    flusher: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[cfg(all(feature = "objectlog", feature = "postgres"))]
fn validate_objectlog_postgres_catalog(
    log: &fireweed_objectlog::ObjectLog,
    projection: &fireweed_postgres::PostgresRelational,
) -> EngineResult<Vec<QueueDefinition>> {
    use fireweed_engine::{LogStore, ProjectionStore};

    let definitions = LogStore::recover_definitions(log)?;
    let log_by_key: HashMap<QueueKey, QueueDefinition> = definitions
        .iter()
        .cloned()
        .map(|definition| {
            (
                QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                definition,
            )
        })
        .collect();
    for projected in ProjectionStore::recover_definitions(projection)? {
        let key = QueueKey::new(projected.tenant_id.clone(), projected.queue_id.clone());
        let Some(authoritative) = log_by_key.get(&key) else {
            return Err(EngineError::Storage(
                "projection contains a queue absent from the authoritative object log".into(),
            ));
        };
        if authoritative != &projected {
            return Err(EngineError::Storage(
                "projection queue definition conflicts with the authoritative object log".into(),
            ));
        }
        let projected_high_water = ProjectionStore::recovery_high_water(projection, &key)?;
        let authoritative_high_water = LogStore::high_water(log, &key)?;
        match (projected_high_water, authoritative_high_water) {
            (Some(_), None) => {
                return Err(EngineError::Storage(
                    "projection is non-empty but the authoritative object log is empty".into(),
                ));
            }
            (Some(projected), Some(authoritative))
                if projected.backend_epoch > authoritative.backend_epoch
                    || (projected.backend_epoch == authoritative.backend_epoch
                        && projected.sequence > authoritative.sequence) =>
            {
                return Err(EngineError::Storage(
                    "projection is ahead of the authoritative object log".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(definitions)
}

#[cfg(all(feature = "objectlog", feature = "postgres"))]
impl ObjectLogPostgresLifecycle {
    fn verify_backend(
        backend: &ObjectLogPostgresBackend,
    ) -> EngineResult<ProjectionVerificationState> {
        use fireweed_engine::{LogStore, ProjectionStore};

        let projection = backend.with_projection(Clone::clone);
        let definitions =
            backend.with_log(|log| validate_objectlog_postgres_catalog(log, &projection))?;
        let mut projection_sequence = 0;
        let mut authoritative_sequence = 0;
        let mut compatible = true;
        for definition in definitions {
            let key = QueueKey::new(definition.tenant_id, definition.queue_id);
            let projected_position = ProjectionStore::recovery_high_water(&projection, &key)?;
            let authoritative_position = backend.with_log(|log| LogStore::high_water(log, &key))?;
            compatible &= projected_position == authoritative_position;
            projection_sequence = projection_sequence.max(
                projected_position
                    .as_ref()
                    .map_or(0, |position| position.sequence),
            );
            authoritative_sequence = authoritative_sequence.max(
                authoritative_position
                    .as_ref()
                    .map_or(0, |position| position.sequence),
            );
        }
        Ok(ProjectionVerificationState {
            compatible,
            projection_sequence,
            authoritative_sequence,
        })
    }

    fn rebuilde_backend(
        backend: &ObjectLogPostgresBackend,
        max_tail_commands: u64,
    ) -> EngineResult<ProjectionRebuildState> {
        use fireweed_engine::{LogStore, ProjectionStore};

        let mut projection = backend.with_projection(Clone::clone);
        let definitions =
            backend.with_log(|log| validate_objectlog_postgres_catalog(log, &projection))?;
        let mut replay = Vec::new();
        for definition in &definitions {
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            ProjectionStore::ensure_shard(&mut projection, definition)?;
            let mut from = projection.recovery_high_water(&key)?;
            loop {
                let page = backend.with_log(|log| log.read_from(&key, from.clone(), 1_024))?;
                replay.extend(page.entries);
                if replay.len() as u64 > max_tail_commands {
                    return Err(EngineError::Storage(format!(
                        "projection rebuilde exceeds configured tail bound {}",
                        max_tail_commands
                    )));
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
        }
        for chunk in replay.chunks(1_024) {
            let positions: Vec<_> = chunk.iter().map(|(position, _)| position.clone()).collect();
            let commands: Vec<_> = chunk.iter().map(|(_, command)| command.clone()).collect();
            projection.apply_recovery(&positions, &commands)?;
        }
        let verification = Self::verify_backend(backend)?;
        Ok(ProjectionRebuildState {
            snapshot_used: false,
            tail_commands_replayed: replay.len() as u64,
            projection_sequence: verification.projection_sequence,
        })
    }
}

#[cfg(all(feature = "objectlog", feature = "postgres"))]
impl ProjectionLifecycle for ObjectLogPostgresLifecycle {
    fn capabilities(&self) -> ProjectionLifecycleCapabilities {
        ProjectionLifecycleCapabilities {
            verify_projection: true,
            delete_projection: true,
            rebuild_projection: true,
        }
    }

    fn verify_projection(&self) -> ProjectionLifecycleFuture<'_, ProjectionVerificationState> {
        let backend = Arc::clone(
            self.backend
                .as_ref()
                .expect("object-log postgres lifecycle is active"),
        );
        Box::pin(
            self.executor
                .run(move || Self::verify_backend(backend.as_ref())),
        )
    }

    fn delete_projection(&self) -> ProjectionLifecycleFuture<'_, ()> {
        let backend = Arc::clone(
            self.backend
                .as_ref()
                .expect("object-log postgres lifecycle is active"),
        );
        Box::pin(self.executor.run(move || {
            backend.with_projection(fireweed_postgres::PostgresRelational::delete_projection)
        }))
    }

    fn rebuild_projection(&self) -> ProjectionLifecycleFuture<'_, ProjectionRebuildState> {
        let backend = Arc::clone(
            self.backend
                .as_ref()
                .expect("object-log postgres lifecycle is active"),
        );
        let max_tail_commands = self.max_tail_commands;
        Box::pin(
            self.executor
                .run(move || Self::rebuilde_backend(backend.as_ref(), max_tail_commands)),
        )
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(flusher) = self.flusher.lock().expect("flusher poisoned").take() {
            let _ = flusher.join();
        }
        if let Some(backend) = self.backend.take() {
            // `postgres::Client::drop` drives its private runtime. Keep the final
            // backend drop off any ambient Tokio runtime just as construction is.
            let _ = std::thread::Builder::new()
                .name("fireweed-objectlog-postgres-drop".to_owned())
                .spawn(move || drop(backend))
                .and_then(|thread| {
                    thread
                        .join()
                        .map_err(|_| std::io::Error::other("postgres drop thread panicked"))
                });
        }
    }
}

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
type ObjectLogSqliteBackend = fireweed_engine::ComposedBackend<
    fireweed_objectlog::ObjectLog,
    fireweed_sqlite::HybridProjectionStore,
    fireweed_engine::InProcessControlPlane,
>;

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
struct ObjectLogSqliteLifecycle {
    backend: Arc<ObjectLogSqliteBackend>,
    executor: blocking_backend::OwnedBlockingExecutor,
    max_tail_commands: u64,
    stop: Arc<std::sync::atomic::AtomicBool>,
    flusher: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
fn validate_objectlog_sqlite_catalog(
    log: &fireweed_objectlog::ObjectLog,
    projection: &fireweed_sqlite::SqliteProjectionStore,
) -> EngineResult<Vec<QueueDefinition>> {
    use fireweed_engine::{LogStore, ProjectionStore};

    let definitions = LogStore::recover_definitions(log)?;
    let log_by_key: HashMap<QueueKey, QueueDefinition> = definitions
        .iter()
        .cloned()
        .map(|definition| {
            (
                QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                definition,
            )
        })
        .collect();
    for projected in ProjectionStore::recover_definitions(projection)? {
        let key = QueueKey::new(projected.tenant_id.clone(), projected.queue_id.clone());
        let Some(authoritative) = log_by_key.get(&key) else {
            return Err(EngineError::Storage(
                "projection contains a queue absent from the authoritative object log".into(),
            ));
        };
        if authoritative != &projected {
            return Err(EngineError::Storage(
                "projection queue definition conflicts with the authoritative object log".into(),
            ));
        }
        let projected_high_water = projection.recovery_high_water(&key)?;
        let authoritative_high_water = LogStore::high_water(log, &key)?;
        match (projected_high_water, authoritative_high_water) {
            (Some(_), None) => {
                return Err(EngineError::Storage(
                    "projection is non-empty but the authoritative object log is empty".into(),
                ));
            }
            (Some(projected), Some(authoritative))
                if projected.backend_epoch > authoritative.backend_epoch
                    || (projected.backend_epoch == authoritative.backend_epoch
                        && projected.sequence > authoritative.sequence) =>
            {
                return Err(EngineError::Storage(
                    "projection is ahead of the authoritative object log".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(definitions)
}

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
fn verify_objectlog_sqlite_axes(
    log: &fireweed_objectlog::ObjectLog,
    projection: &fireweed_sqlite::HybridProjectionStore,
    require_online: bool,
) -> EngineResult<ProjectionVerificationState> {
    use fireweed_engine::{LogStore, ProjectionStore};

    if require_online && projection.durable_projection_offline() {
        return Err(EngineError::Storage(
            "SQLite projection is offline pending authoritative rebuild".into(),
        ));
    }
    if require_online && let Some(reason) = projection.checkpoint_error() {
        return Err(EngineError::Storage(format!(
            "async SQLite checkpoint worker failed: {reason}"
        )));
    }
    if require_online && let Some(reason) = projection.poison_reason() {
        return Err(EngineError::Storage(format!(
            "hybrid projection worker is poisoned: {reason}"
        )));
    }
    let definitions = LogStore::recover_definitions(log)?;
    let authoritative: HashMap<QueueKey, QueueDefinition> = definitions
        .iter()
        .cloned()
        .map(|definition| {
            (
                QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                definition,
            )
        })
        .collect();
    let projected: HashMap<QueueKey, QueueDefinition> =
        ProjectionStore::recover_definitions(projection.sqlite())?
            .into_iter()
            .map(|definition| {
                (
                    QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                    definition,
                )
            })
            .collect();
    if authoritative != projected {
        return Err(EngineError::Storage(
            "SQLite queue catalog does not exactly match the authoritative object log".into(),
        ));
    }

    let mut projection_sequence = 0;
    let mut authoritative_sequence = 0;
    for definition in definitions {
        let key = QueueKey::new(definition.tenant_id, definition.queue_id);
        let projected_position = projection.sqlite().recovery_high_water(&key)?;
        let authoritative_position = LogStore::high_water(log, &key)?;
        if projected_position != authoritative_position {
            return Err(EngineError::Storage(format!(
                "SQLite projection for {}/{} is not at the authoritative position: projection {:?}, log {:?}",
                key.tenant_id, key.queue_id, projected_position, authoritative_position
            )));
        }
        projection_sequence = projection_sequence.max(
            projected_position
                .as_ref()
                .map_or(0, |position| position.sequence),
        );
        authoritative_sequence = authoritative_sequence.max(
            authoritative_position
                .as_ref()
                .map_or(0, |position| position.sequence),
        );
    }
    Ok(ProjectionVerificationState {
        compatible: true,
        projection_sequence,
        authoritative_sequence,
    })
}

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
impl ObjectLogSqliteLifecycle {
    fn verify_backend(
        backend: &ObjectLogSqliteBackend,
    ) -> EngineResult<ProjectionVerificationState> {
        backend
            .with_quiesced_log_and_projection_mut(|log, projection| {
                // Async writes may have advanced the authoritative log while their bounded SQLite
                // checkpoint remains queued. Drain every admitted chunk under the lifecycle lock before
                // comparing axes. Keep this barrier verification-specific: repair paths must be able to
                // discard and rebuild a poisoned deferred checkpoint.
                loop {
                    let before = projection.deferred_command_count();
                    if before == 0 {
                        break;
                    }
                    fireweed_engine::ProjectionStore::flush_deferred(projection)?;
                    let after = projection.deferred_command_count();
                    if after >= before {
                        return Err(EngineError::Storage(
                            "deferred SQLite checkpoint made no progress during projection verification"
                                .into(),
                        ));
                    }
                }
                verify_objectlog_sqlite_axes(log, projection, true)
            })
    }

    fn rebuilde_backend(
        backend: &ObjectLogSqliteBackend,
        max_tail_commands: u64,
    ) -> EngineResult<ProjectionRebuildState> {
        use fireweed_engine::LogStore;

        backend.with_quiesced_log_and_projection_mut(|log, projection| {
            projection.begin_durable_rebuild()?;
            let definitions = LogStore::recover_definitions(log)?;
            let mut replayed = 0_u64;
            for definition in &definitions {
                let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
                projection
                    .sqlite()
                    .create_queue_projection(definition.clone())?;
                let mut from = None;
                loop {
                    let page = log.read_from(&key, from.clone(), 1_024)?;
                    replayed = replayed.saturating_add(page.entries.len() as u64);
                    if replayed > max_tail_commands {
                        return Err(EngineError::Storage(format!(
                            "projection rebuilde exceeds configured tail bound {}",
                            max_tail_commands
                        )));
                    }
                    if !page.entries.is_empty() {
                        let positions: Vec<_> = page
                            .entries
                            .iter()
                            .map(|(position, _)| position.clone())
                            .collect();
                        let commands: Vec<_> = page
                            .entries
                            .iter()
                            .map(|(_, command)| command.clone())
                            .collect();
                        projection
                            .sqlite()
                            .apply_committed_batch(&positions, &commands)?;
                    }
                    match page.next {
                        Some(next) => from = Some(next),
                        None => break,
                    }
                }
            }
            let verification = verify_objectlog_sqlite_axes(log, projection, false)?;
            projection.finish_durable_rebuild();
            Ok(ProjectionRebuildState {
                snapshot_used: false,
                tail_commands_replayed: replayed,
                projection_sequence: verification.projection_sequence,
            })
        })
    }
}

#[cfg(all(feature = "objectlog", feature = "sqlite"))]
impl ProjectionLifecycle for ObjectLogSqliteLifecycle {
    fn capabilities(&self) -> ProjectionLifecycleCapabilities {
        ProjectionLifecycleCapabilities {
            verify_projection: true,
            delete_projection: true,
            rebuild_projection: true,
        }
    }

    fn buffered_group_commit_commands(&self) -> Option<usize> {
        Some(self.backend.buffered_group_commit_commands())
    }

    fn verify_projection(&self) -> ProjectionLifecycleFuture<'_, ProjectionVerificationState> {
        let backend = Arc::clone(&self.backend);
        Box::pin(
            self.executor
                .run(move || Self::verify_backend(backend.as_ref())),
        )
    }

    fn delete_projection(&self) -> ProjectionLifecycleFuture<'_, ()> {
        let backend = Arc::clone(&self.backend);
        Box::pin(self.executor.run(move || {
            backend.with_quiesced_log_and_projection_mut(|_, projection| {
                projection.delete_durable_projection()
            })
        }))
    }

    fn rebuild_projection(&self) -> ProjectionLifecycleFuture<'_, ProjectionRebuildState> {
        let backend = Arc::clone(&self.backend);
        let max_tail_commands = self.max_tail_commands;
        Box::pin(
            self.executor
                .run(move || Self::rebuilde_backend(backend.as_ref(), max_tail_commands)),
        )
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(flusher) = self.flusher.lock().expect("flusher poisoned").take() {
            let _ = flusher.join();
        }
    }
}

#[cfg(feature = "objectlog")]
fn open_composed_object_log(
    config: &ComposedStorageConfig,
    authoritative: bool,
) -> EngineResult<fireweed_objectlog::ObjectLog> {
    use fireweed_objectlog::segmented::{
        BlobStore, LocalFsBlobStore, NamespacedBlobStore, S3BlobStore,
    };

    let segment_config = fireweed_objectlog::SegmentConfig::new(
        config.segments.target_bytes,
        config.segments.max_latency_ms,
    )?;
    let probe_native_s3 = matches!(&config.object_log, ObjectLogConfig::S3Compatible { .. })
        && matches!(
            &config.object_log_authority,
            ObjectLogAuthorityConfig::NativeConditionalWrite
        );
    let raw: Arc<dyn BlobStore> = match &config.object_log {
        ObjectLogConfig::Local { root } => Arc::new(LocalFsBlobStore::open(root)?),
        ObjectLogConfig::S3Compatible {
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            allow_insecure_http,
        } => {
            if endpoint.starts_with("http://") && !allow_insecure_http {
                return Err(EngineError::Invalid(
                    "insecure S3-compatible HTTP endpoint was not explicitly allowed",
                ));
            }
            Arc::new(S3BlobStore::new(
                endpoint,
                bucket,
                &access_key_id.0,
                &secret_access_key.0,
                region,
            )?)
        }
    };
    let raw: Arc<dyn BlobStore> = match &config.object_log_authority {
        ObjectLogAuthorityConfig::NativeConditionalWrite => raw,
        ObjectLogAuthorityConfig::Postgres { url } => {
            #[cfg(feature = "postgres")]
            {
                use fireweed_objectlog::segmented::PointerFencedBlobStore;
                let pointers = Arc::new(fireweed_postgres::PostgresManifestPointer::open(&url.0)?);
                Arc::new(PointerFencedBlobStore::new(raw, pointers))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = url;
                return Err(EngineError::Unavailable);
            }
        }
    };
    let namespace = object_log_namespace(&config.namespace);
    let store: Arc<dyn BlobStore> = Arc::new(NamespacedBlobStore::new(raw, &namespace)?);
    if probe_native_s3 {
        fireweed_objectlog::segmented::probe_create_only_semantics(store.as_ref())?;
    }
    if authoritative {
        fireweed_objectlog::ObjectLog::open_group_commit_authoritative_with_blob_store(
            store,
            segment_config,
        )
    } else {
        fireweed_objectlog::ObjectLog::open_group_commit_with_blob_store(store, segment_config)
    }
}

/// The capabilities the library facade composes over (the worker + control-plane ports). This is an
/// INTERNAL composition bound, not a consumer-facing trait: a backend satisfies it automatically (blanket
/// impl over the engine ports) and a consumer never names or implements it. Hidden from the public docs.
#[doc(hidden)]
pub(crate) trait LibBackend:
    Backend
    + PushPort
    + ClaimPort
    + UpsertPort
    + UpdateFieldsPort
    + FinalizePort
    + CommitTransitionPort
    + RecoveryReadPort
    + RenewLeasePort
    + ReassignLeasePort
    + ReclaimPort
    + ReschedulePort
    + PurgePort
    + SetGatesPort
    + ProjectionRead
    + HistoricalProjectionRead
    + IndexQueryPort
    + DiscoveryPort
    + HotProjectionQueryPort
    + ControlPlaneStore
    + Send
    + Sync
{
}
#[doc(hidden)]
impl<T> LibBackend for T where
    T: Backend
        + PushPort
        + ClaimPort
        + UpsertPort
        + UpdateFieldsPort
        + FinalizePort
        + CommitTransitionPort
        + RecoveryReadPort
        + RenewLeasePort
        + ReassignLeasePort
        + ReclaimPort
        + ReschedulePort
        + PurgePort
        + SetGatesPort
        + ProjectionRead
        + HistoricalProjectionRead
        + IndexQueryPort
        + DiscoveryPort
        + HotProjectionQueryPort
        + ControlPlaneStore
        + Send
        + Sync
{
}

/// Serialize a [`serde_json::Value`] to the axon_esf-compatible raw byte format expected by the
/// typed-index lookup path in the projection (`decode_typed_lookup_value`):
/// - `String`: raw UTF-8 bytes (no JSON quoting) — matches String and Datetime index types.
/// - `Number` / `Bool` / other: JSON-encoded bytes — matches Integer, Float, and Boolean index types.
fn json_value_to_index_key_bytes(value: &serde_json::Value) -> Vec<u8> {
    match value {
        serde_json::Value::String(s) => s.as_bytes().to_vec(),
        other => serde_json::to_vec(other).expect("infallible JSON serialization"),
    }
}

fn typed_index_query_key_bytes(
    spec: &QueueIndex,
    key_values: &[serde_json::Value],
) -> EngineResult<Vec<Vec<u8>>> {
    let expected_arity = match &spec.declaration {
        IndexDeclaration::Single(_) => 1,
        IndexDeclaration::Compound(def) => def.fields.len(),
    };
    if key_values.len() != expected_arity {
        return Err(EngineError::Invalid("secondary index key arity mismatch"));
    }

    let mut raw = Vec::with_capacity(key_values.len());
    match &spec.declaration {
        IndexDeclaration::Single(def) => {
            encode_index_value(&key_values[0], &def.index_type).map_err(|_| {
                EngineError::Invalid("typed index value is not valid for declared type")
            })?;
            raw.push(json_value_to_index_key_bytes(&key_values[0]));
        }
        IndexDeclaration::Compound(def) => {
            for (value, field) in key_values.iter().zip(def.fields.iter()) {
                encode_index_value(value, &field.index_type).map_err(|_| {
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?;
                raw.push(json_value_to_index_key_bytes(value));
            }
        }
    }

    Ok(raw)
}

fn typed_index_unique(spec: &QueueIndex) -> bool {
    match &spec.declaration {
        IndexDeclaration::Single(def) => def.unique,
        IndexDeclaration::Compound(def) => def.unique,
    }
}

/// `ts + millis`, normalizing nanoseconds — derives a lease expiry from `now`.
fn add_millis(ts: UtcTimestamp, millis: u64) -> UtcTimestamp {
    let total =
        ts.seconds as i128 * 1_000_000_000 + ts.nanoseconds as i128 + millis as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

/// A claim whose two times are decided by the caller instead of both being read off this handle's
/// [`Clock`] ([`Fireweed::claim_at`] / [`Fireweed::claim_response_at`]).
///
/// [`Fireweed::claim`] takes ONE `Clock::now` reading and uses it for both jobs a claim needs a time for,
/// which is exactly right for a worker draining a queue in real time. Scheduled work is the case it
/// cannot express: selecting the items due at some execution epoch (a backfill, a replay, a scheduler
/// tick resolved slightly in the past or the future) while the leases it hands out must still be valid
/// against the *operational* clock. The two times are therefore separate here:
///
/// * `eligibility_time` — the epoch due-ness is resolved at: an item is selected when its
///   `not_before` (its scheduled time) is `<= eligibility_time`, so the boundary is inclusive and an
///   item scheduled exactly AT this instant is claimed. It is a selection input only: nothing is
///   stamped with it. `None` ⇒ the operational time below.
/// * `lease_time` — the epoch the lease is measured from: the claimed items expire at
///   `lease_time + lease_ms`, and the claim's command is stamped with it. `None` ⇒ this handle's
///   `Clock::now()`, which is what a caller wants even when `eligibility_time` is far from it.
///
/// Leaving both `None` is precisely [`Fireweed::claim_with`], so an unset field never changes behaviour.
///
/// ```no_run
/// # use fireweed::{ClaimAt, EngineResult, Fireweed, QueueKey, UtcTimestamp};
/// # async fn f(fireweed: &Fireweed, queue: &QueueKey, tick: UtcTimestamp) -> EngineResult<()> {
/// // Work scheduled for `tick`, leased for 60s against the real clock.
/// let due = fireweed.claim_at(queue, ClaimAt::new(100, 60_000).eligibility_time(tick)).await?;
/// # let _ = due;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct ClaimAt {
    /// Maximum items to lease (the `max` of [`Fireweed::claim`]).
    pub max: usize,
    /// Lease duration in milliseconds, measured from `lease_time`.
    pub lease_ms: u64,
    /// The epoch to resolve due-ness at (`not_before <= eligibility_time`). `None` ⇒ `lease_time`.
    pub eligibility_time: Option<UtcTimestamp>,
    /// The epoch the lease is measured from. `None` ⇒ this handle's `Clock::now()`.
    pub lease_time: Option<UtcTimestamp>,
    /// API-001 compatibility options, as for [`Fireweed::claim_with`].
    pub compatibility: ClaimCompatibility,
}

const MAX_MULTI_QUEUE_CLAIM_TARGETS: usize = 16;
const MAX_MULTI_QUEUE_CLAIM_ITEMS: usize = 1024;

/// One independently committed queue claim in [`Fireweed::claim_across_queues`].
#[derive(Debug, Clone)]
pub struct MultiQueueClaimTarget {
    /// Queue to claim from.
    pub queue: QueueKey,
    /// Claim parameters. `lease_time` must be unset so the facade can use one common instant.
    pub claim: ClaimAt,
}

/// Caller-selected safety limits for [`Fireweed::claim_across_queues`].
///
/// The defaults are the largest accepted values. Callers may lower either positive limit, but cannot
/// raise the fixed ceilings of 16 queues and 1024 aggregate requested items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiQueueClaimLimits {
    pub max_targets: usize,
    pub max_total_items: usize,
}

impl Default for MultiQueueClaimLimits {
    fn default() -> Self {
        Self {
            max_targets: MAX_MULTI_QUEUE_CLAIM_TARGETS,
            max_total_items: MAX_MULTI_QUEUE_CLAIM_ITEMS,
        }
    }
}

/// The result of one target in [`Fireweed::claim_across_queues`].
///
/// Runtime failures are retained beside their queue instead of failing or truncating the outer call.
#[derive(Debug)]
pub struct MultiQueueClaimResult {
    pub queue: QueueKey,
    pub result: EngineResult<Claimed>,
}

impl ClaimAt {
    /// A claim of up to `max` items leased for `lease_ms`, with both times defaulted (identical to
    /// [`Fireweed::claim`] until an explicit time is set).
    pub fn new(max: usize, lease_ms: u64) -> Self {
        Self {
            max,
            lease_ms,
            ..Self::default()
        }
    }

    /// Resolve due-ness at `at` instead of the operational clock. See [`ClaimAt::eligibility_time`].
    pub fn eligibility_time(mut self, at: UtcTimestamp) -> Self {
        self.eligibility_time = Some(at);
        self
    }

    /// Measure the lease from `at` instead of this handle's `Clock::now()`. See [`ClaimAt::lease_time`].
    pub fn lease_time(mut self, at: UtcTimestamp) -> Self {
        self.lease_time = Some(at);
        self
    }

    /// Attach API-001 compatibility options (group batching / whole cohort / …).
    pub fn compatibility(mut self, compatibility: ClaimCompatibility) -> Self {
        self.compatibility = compatibility;
        self
    }
}

/// Query-claim timing overrides, mirroring [`ClaimAt`] for [`Fireweed::claim_by_query_at`].
///
/// `eligibility_time` resolves due-ness for the declared-index selection.
/// `lease_time` stamps the command and anchors the lease expiry.
#[derive(Debug, Clone, Default)]
pub struct ClaimByQueryAt {
    /// The epoch to resolve due-ness at (`not_before <= eligibility_time`). `None` ⇒ `lease_time`.
    pub eligibility_time: Option<UtcTimestamp>,
    /// The epoch the lease is measured from. `None` ⇒ this handle's `Clock::now()`.
    pub lease_time: Option<UtcTimestamp>,
}

impl ClaimByQueryAt {
    /// Build a query claim with both timestamps defaulted.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve due-ness at `at` instead of the operational clock.
    pub fn eligibility_time(mut self, at: UtcTimestamp) -> Self {
        self.eligibility_time = Some(at);
        self
    }

    /// Measure the lease from `at` instead of this handle's `Clock::now()`.
    pub fn lease_time(mut self, at: UtcTimestamp) -> Self {
        self.lease_time = Some(at);
        self
    }
}

/// How a `nack` returns an in-flight item: back to the queue for another attempt (`Retry`) or released
/// to a fresh delivery without charging the failure differently (`Release`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nack {
    /// Return to Pending for re-claim. `not_before` is an optional **queue-native retry backoff**: the item
    /// stays ineligible until that absolute timestamp. `None` re-eligibles it immediately. (Use
    /// [`Fireweed::nack_retry_after`] for a relative delay.)
    Retry {
        not_before: Option<UtcTimestamp>,
    },
    Release,
}

/// Who currently owns a queue, from a coordinated handle's view (ADR-009 L5 — the value form of the RESP
/// `-MOVED` redirect). A sole-owner handle is always [`Ownership::Mine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ownership {
    /// This instance is the live owner (or a sole-owner handle). `epoch` is the current assignment epoch
    /// (`None` for a sole-owner handle, or a coordinated owner whose queue has no granted lease yet).
    Mine { epoch: Option<u64> },
    /// A DIFFERENT live instance owns the queue — route there (the value form of `-MOVED`).
    Elsewhere { owner: OwnerId, epoch: Option<u64> },
    /// No live owner holds the queue right now (unassigned / expired).
    Unowned,
}

/// An item to enqueue. For [`Fireweed::push`], `client_item_key` is optional and defaults to the
/// server-assigned id when omitted; for [`Fireweed::upsert`], the caller supplies the dedup key as the
/// method argument.
#[derive(Debug, Clone, Default)]
pub struct NewItem {
    pub client_item_key: Option<ClientItemKey>,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    /// Declared cohort size (BQ-14c) — see [`ClaimCompatibility`]/`whole_cohort`. `None` for non-cohort items.
    pub cohort_size: Option<u64>,
    /// Gate keys this item carries (BQ-14d). A blocked gate key makes the item ineligible. Empty = un-gated.
    pub gate_keys: Vec<String>,
    /// Typed JSON entity document (ADR-011). Present for schema-validated typed queues; absent for
    /// schema-less queues that use the opaque `payload` bytes carrier instead. When both are present, the
    /// `entity` is the canonical typed representation (used by schema validation and axon_esf index-key
    /// computation); `payload` is preserved for legacy/schema-less callers and stored independently.
    pub entity: Option<serde_json::Value>,
}

/// Map a public [`NewItem`] to the engine's [`PushSpec`] (shared by `push` and `commit`).
fn new_item_to_spec(it: NewItem) -> PushSpec {
    PushSpec {
        client_item_key: it.client_item_key,
        priority: it.priority,
        not_before: it.not_before,
        group_key: it.group_key,
        payload: it.payload,
        fields: it.fields,
        metadata: it.metadata,
        cohort_size: it.cohort_size,
        gate_keys: it.gate_keys,
        entity: it.entity,
    }
}

/// One entry of a vectorized claimed-work [`CommitRequest`] (Snorri transition commit, epic
/// pqueue-2201fd37): atomically validate `claim_ref` (lease token + version fence), write the opaque
/// non-work `side_records`, enqueue `lifecycle_items` as ordinary dispatchable work, and finalize the input
/// claim with `finalize`.
#[derive(Debug, Clone)]
pub struct CommitEntry {
    pub claim_ref: ClaimRef,
    pub finalize: FinalizeKind,
    pub side_records: Vec<SideRecord>,
    pub lifecycle_items: Vec<NewItem>,
    /// Optional caller-supplied instance/state fence advanced/validated atomically with this entry (C6,
    /// epic pqueue-2201fd37). The entry commits only if the queue's stored fence for `instance_key` equals
    /// `expected` (absent reads as `0`) and `next > expected`; on a stale `expected` the entry is rejected
    /// `Conflict` (nothing written), on `next <= expected` rejected `Invalid`. Defaults to `None` (no fence).
    pub instance_fence: Option<InstanceFence>,
}

/// A vectorized claimed-work commit (Snorri authoritative StateStore boundary). `request_id` drives
/// retained replay/conflict/expired idempotency over the WHOLE body; `entries` are applied with independent
/// per-entry outcomes (all-or-nothing is NOT required across entries, but each entry's writes are atomic).
#[derive(Debug, Clone, Default)]
pub struct CommitRequest {
    pub request_id: Option<RequestId>,
    pub entries: Vec<CommitEntry>,
}

/// One atomic transition that consumes more than one claimed work item. This is the result/await
/// continuation shape: every claim is validated before the projection, fence, continuation, or any
/// finalization becomes visible. `additional_claim_refs` may be empty, although [`CommitEntry`] is simpler
/// for that case.
#[derive(Debug, Clone)]
pub struct MultiClaimCommitEntry {
    pub claim_ref: ClaimRef,
    pub additional_claim_refs: Vec<ClaimRef>,
    pub finalize: FinalizeKind,
    pub side_records: Vec<SideRecord>,
    pub lifecycle_items: Vec<NewItem>,
    pub instance_fence: Option<InstanceFence>,
}

/// A vectorized request whose individual entries may atomically consume multiple claims.
#[derive(Debug, Clone, Default)]
pub struct MultiClaimCommitRequest {
    pub request_id: Option<RequestId>,
    pub entries: Vec<MultiClaimCommitEntry>,
}

/// The per-entry result of a [`Fireweed::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryOutcome {
    /// The entry validated and committed atomically. `lifecycle_item_ids` are the server-assigned ids of the
    /// entry's newly enqueued dispatchable items, in order (empty when the entry enqueued none).
    Committed { lifecycle_item_ids: Vec<ItemId> },
    /// The entry's `claim_ref` (or a lifecycle write) was rejected; NOTHING was mutated for this entry.
    Rejected(EngineError),
}

/// How a [`RuntimeCore`] handle coordinates ownership (ADR-009 / TD-003 In-Process Library Owner-Runtime).
enum Coordination {
    /// Degenerate sole-owner: no control plane, constant ownership, never fences (`expected_epoch = None`).
    /// This is the default and keeps single-instance behaviour byte-identical.
    Sole,
    /// A coordinated owner over a shared control plane. Each queue-addressed op operates under an acquired,
    /// epoch-fenced [`OwnedSession`] (cached per queue), so a superseded instance self-fences on the data
    /// path. `acquire_and_fence` advances the storage fence epoch the op stamps.
    Owner {
        owner_id: OwnerId,
        control_plane: Arc<dyn QueueControlPlane>,
        /// Present for synchronous durable control planes. It owns each full
        /// authority sequence, including its async storage-fence phase.
        control_plane_runtime: Option<Arc<dyn OwnedControlPlaneRuntime>>,
        sessions: Mutex<HashMap<QueueKey, OwnedSession>>,
        /// Queues observed `Draining` on the renew loop (TD-003 §Graceful Drain). While a queue is here the
        /// owner serves in-flight ops but refuses a NEW claim with a retryable `Unavailable` (drain split).
        draining: Mutex<HashSet<QueueKey>>,
    },
}

type OwnedControlPlaneFuture<'a, T> = Pin<Box<dyn Future<Output = EngineResult<T>> + Send + 'a>>;

trait OwnedControlPlaneRuntime: Send + Sync {
    fn establish(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        now: UtcTimestamp,
    ) -> OwnedControlPlaneFuture<'_, OwnershipOutcome>;
    fn resolve(
        &self,
        queue: QueueKey,
        now: UtcTimestamp,
    ) -> OwnedControlPlaneFuture<'_, fireweed_engine::OwnerResolution>;
    fn renew(
        &self,
        owner: OwnerId,
        renewals: Vec<fireweed_engine::LeaseRenewal>,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<fireweed_engine::LeaseRenewalOutcome>>;
}

#[cfg(any(feature = "postgres", test))]
struct BlockingControlPlaneRuntime<B: LibBackend + 'static> {
    backend: Arc<B>,
    control_plane: Mutex<Option<Arc<dyn QueueControlPlane>>>,
    executor: blocking_backend::OwnedBlockingExecutor,
}

#[cfg(any(feature = "postgres", test))]
impl<B: LibBackend + 'static> BlockingControlPlaneRuntime<B> {
    fn control_plane(&self) -> Arc<dyn QueueControlPlane> {
        Arc::clone(
            self.control_plane
                .lock()
                .expect("control-plane runtime poisoned")
                .as_ref()
                .expect("control-plane runtime is active"),
        )
    }
}

#[cfg(any(feature = "postgres", test))]
impl<B: LibBackend + 'static> Drop for BlockingControlPlaneRuntime<B> {
    fn drop(&mut self) {
        let Some(control_plane) = self
            .control_plane
            .lock()
            .expect("control-plane runtime poisoned")
            .take()
        else {
            return;
        };
        let _ = std::thread::Builder::new()
            .name("fireweed-control-plane-drop".to_owned())
            .spawn(move || drop(control_plane))
            .and_then(|thread| {
                thread
                    .join()
                    .map_err(|_| std::io::Error::other("control-plane drop thread panicked"))
            });
    }
}

#[cfg(any(feature = "postgres", test))]
impl<B: LibBackend + 'static> OwnedControlPlaneRuntime for BlockingControlPlaneRuntime<B> {
    fn establish(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        now: UtcTimestamp,
    ) -> OwnedControlPlaneFuture<'_, OwnershipOutcome> {
        let backend = Arc::clone(&self.backend);
        let control_plane = self.control_plane();
        let executor = self.executor.clone();
        Box::pin(async move {
            executor
                .run_for_control_plane_queue(&queue.clone(), move || {
                    control_plane.register_owner(&owner, now)?;
                    let resolution = control_plane.resolve_queue_owner(&queue, now)?;
                    if resolution
                        .active_owner
                        .as_ref()
                        .is_some_and(|active| active != &owner)
                    {
                        return Err(EngineError::Forbidden("queue owned by another live owner"));
                    }
                    if resolution.target_owner.as_ref() != Some(&owner) {
                        return Err(EngineError::Forbidden("queue targets another owner"));
                    }
                    futures::executor::block_on(acquire_and_fence(
                        control_plane.as_ref(),
                        backend.as_ref(),
                        &queue,
                        &owner,
                        now,
                    ))
                })
                .await
        })
    }

    fn resolve(
        &self,
        queue: QueueKey,
        now: UtcTimestamp,
    ) -> OwnedControlPlaneFuture<'_, fireweed_engine::OwnerResolution> {
        let control_plane = self.control_plane();
        let executor = self.executor.clone();
        Box::pin(async move {
            executor
                .run_for_control_plane_queue(&queue.clone(), move || {
                    control_plane.resolve_queue_owner(&queue, now)
                })
                .await
        })
    }

    fn renew(
        &self,
        owner: OwnerId,
        renewals: Vec<fireweed_engine::LeaseRenewal>,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<fireweed_engine::LeaseRenewalOutcome>> {
        let control_plane = self.control_plane();
        futures::executor::block_on(self.executor.run_control_plane(move || {
            control_plane.heartbeat(&owner, now)?;
            control_plane.renew_queue_leases(&renewals, now)
        }))
    }
}

/// The ergonomic library handle. Holds an injected backend + clock; generates ids/lease tokens.
#[doc(hidden)]
pub(crate) struct RuntimeCore<B> {
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    ids: AtomicU64,
    coordination: Coordination,
}

fn apply_owned_renewal_outcomes(
    sessions: &Mutex<HashMap<QueueKey, OwnedSession>>,
    draining: &Mutex<HashSet<QueueKey>>,
    owned: Vec<(QueueKey, u64)>,
    outcomes: Vec<fireweed_engine::LeaseRenewalOutcome>,
) -> EngineResult<()> {
    let mut first_error = None;
    for ((queue, _lease_epoch), outcome) in owned.into_iter().zip(outcomes) {
        match outcome {
            fireweed_engine::LeaseRenewalOutcome::Renewed(lease) => {
                let mut draining = draining.lock().expect("poisoned");
                if lease.state == LeaseState::Draining {
                    draining.insert(queue);
                } else {
                    draining.remove(&queue);
                }
            }
            fireweed_engine::LeaseRenewalOutcome::Fenced
            | fireweed_engine::LeaseRenewalOutcome::Missing => {
                draining.lock().expect("poisoned").remove(&queue);
                sessions.lock().expect("poisoned").remove(&queue);
            }
            fireweed_engine::LeaseRenewalOutcome::Error(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

impl<B: LibBackend> RuntimeCore<B> {
    /// Low-level backend-injection constructor for a **sole-owner** handle. Hidden from the published
    /// surface (ADR-009 §4a / L6): external clients build via [`open_memory`]/[`open_sqlite`]/
    /// [`open_sqlite_relational`]/
    /// [`open_objectlog`], which construct the backend internally so a port-bearing handle is never named.
    /// First-party crates/tests that inject a concrete backend use this.
    #[doc(hidden)]
    pub fn new(backend: Arc<B>, clock: Arc<dyn Clock>) -> Self {
        Self {
            backend,
            clock,
            ids: AtomicU64::new(0),
            coordination: Coordination::Sole,
        }
    }

    /// A **durable multi-instance** coordinated owner over a shared control plane (ADR-009 / TD-003).
    /// `instance_id` is THIS instance's unique id — passing it *declares a multi-instance deployment*
    /// (omit it, via [`RuntimeCore::new`]/`open_*`, for a single-instance deployment). Every queue-addressed op
    /// resolves ownership and operates under an acquired, epoch-fenced session, so a superseded instance is
    /// rejected `EpochFenced` at commit.
    ///
    /// **Fencing model (ADR-009 / TD-003):** the append-fence epoch is owned authoritatively by the
    /// *storage backend* (`acquire_epoch`), not the control plane — so cross-process competition is safe as
    /// long as the control plane and the backend are both **shared** across the instances (e.g. a postgres
    /// control plane paired with a postgres backend over one database). A non-shared (in-memory) control
    /// plane only coordinates handles within one process; passing one here is admissible but does not give
    /// cross-process competition. Returns `EngineResult` for signature stability — it does not currently
    /// reject (the removed `binds_storage_epoch` capability gate is obsolete now that storage owns the fence).
    ///
    /// Hidden from the public docs: the blessed coordinated path is [`open_postgres_coordinated`], which
    /// builds the control plane internally so a consumer never names [`QueueControlPlane`]. This lower-level
    /// constructor (bring-your-own control plane) remains available for advanced/custom planes.
    #[doc(hidden)]
    #[allow(dead_code)] // Private compatibility constructor exercised only by crate-internal tests.
    pub fn with_control_plane(
        backend: Arc<B>,
        clock: Arc<dyn Clock>,
        instance_id: OwnerId,
        control_plane: Arc<dyn QueueControlPlane>,
    ) -> EngineResult<Self> {
        Ok(Self::with_control_plane_in_process(
            backend,
            clock,
            instance_id,
            control_plane,
        ))
    }

    /// In-process coordinated owner **without** the durable-capability check — for in-process coordination
    /// *logic* (tests, single-process multi-handle), where the in-memory reference control plane is
    /// admissible non-durably (N4a). Hidden from the published surface; durable deployments use
    /// [`RuntimeCore::with_control_plane`].
    #[doc(hidden)]
    #[allow(dead_code)] // Private compatibility constructor exercised only by crate-internal tests.
    pub fn with_control_plane_in_process(
        backend: Arc<B>,
        clock: Arc<dyn Clock>,
        instance_id: OwnerId,
        control_plane: Arc<dyn QueueControlPlane>,
    ) -> Self {
        Self {
            backend,
            clock,
            ids: AtomicU64::new(0),
            coordination: Coordination::Owner {
                owner_id: instance_id,
                control_plane,
                control_plane_runtime: None,
                sessions: Mutex::new(HashMap::new()),
                draining: Mutex::new(HashSet::new()),
            },
        }
    }

    #[cfg(any(feature = "postgres", test))]
    fn with_owned_control_plane_executor(
        backend: Arc<B>,
        clock: Arc<dyn Clock>,
        instance_id: OwnerId,
        control_plane: Arc<dyn QueueControlPlane>,
        control_plane_executor: blocking_backend::OwnedBlockingExecutor,
    ) -> Self
    where
        B: 'static,
    {
        let control_plane_runtime: Arc<dyn OwnedControlPlaneRuntime> =
            Arc::new(BlockingControlPlaneRuntime {
                backend: Arc::clone(&backend),
                control_plane: Mutex::new(Some(Arc::clone(&control_plane))),
                executor: control_plane_executor,
            });
        Self {
            backend,
            clock,
            ids: AtomicU64::new(0),
            coordination: Coordination::Owner {
                owner_id: instance_id,
                control_plane,
                control_plane_runtime: Some(control_plane_runtime),
                sessions: Mutex::new(HashMap::new()),
                draining: Mutex::new(HashSet::new()),
            },
        }
    }

    fn next(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::SeqCst)
    }

    /// The fence epoch to stamp for `queue`: `None` for a sole-owner handle; `Some(cached fence_epoch)` for
    /// a coordinated owner — acquiring-and-fencing on first use and caching the [`OwnedSession`]. Returns
    /// `Forbidden` when a different live owner holds the queue (the explicit owned-elsewhere value form is
    /// added in a later step). A superseded owner keeps its cached (now-stale) epoch, so its next data-plane
    /// op self-fences `EpochFenced` — fail-closed on the data path independent of the control-plane loop.
    async fn session_epoch(&self, queue: &QueueKey) -> EngineResult<Option<u64>> {
        self.session_epoch_with_time(queue, None).await
    }

    /// Resolve the fence epoch using a caller-supplied operational time when ownership must be
    /// established. A cached session needs no time and is returned unchanged.
    async fn session_epoch_at(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<Option<u64>> {
        self.session_epoch_with_time(queue, Some(now)).await
    }

    async fn session_epoch_with_time(
        &self,
        queue: &QueueKey,
        now: Option<UtcTimestamp>,
    ) -> EngineResult<Option<u64>> {
        let Coordination::Owner {
            owner_id,
            control_plane,
            control_plane_runtime,
            sessions,
            ..
        } = &self.coordination
        else {
            return Ok(None);
        };
        if let Some(s) = sessions.lock().expect("poisoned").get(queue) {
            return Ok(Some(s.fence_epoch));
        }
        let now = now.unwrap_or_else(|| self.clock.now());
        let outcome = if let Some(runtime) = control_plane_runtime {
            runtime
                .establish(queue.clone(), owner_id.clone(), now)
                .await?
        } else {
            control_plane.register_owner(owner_id, now)?;
            let res = control_plane.resolve_queue_owner(queue, now)?;
            // A DIFFERENT live owner holds the queue → owned elsewhere; never contend a live lease.
            if res
                .active_owner
                .as_ref()
                .is_some_and(|active| active != owner_id)
            {
                return Err(EngineError::Forbidden("queue owned by another live owner"));
            }
            if res.target_owner.as_ref() != Some(owner_id) {
                return Err(EngineError::Forbidden("queue targets another owner"));
            }
            acquire_and_fence(
                control_plane.as_ref(),
                self.backend.as_ref(),
                queue,
                owner_id,
                now,
            )
            .await?
        };
        match outcome {
            OwnershipOutcome::Owned(session) => {
                let epoch = session.fence_epoch;
                sessions
                    .lock()
                    .expect("poisoned")
                    .insert(queue.clone(), session);
                Ok(Some(epoch))
            }
            OwnershipOutcome::Rejected(_) => {
                Err(EngineError::Forbidden("queue owned by another live owner"))
            }
        }
    }

    /// Drop the cached session for `queue` so the next op re-resolves ownership. Called when a data-plane op
    /// is `EpochFenced` — a fenced owner has been superseded, so its stale session must not be reused (it
    /// will re-resolve and discover it is owned elsewhere). Sole-owner is a no-op.
    fn invalidate_session(&self, queue: &QueueKey) {
        if let Coordination::Owner { sessions, .. } = &self.coordination {
            sessions.lock().expect("poisoned").remove(queue);
        }
    }

    /// Drop the cached session on `EpochFenced` (re-resolve next op), then return the result unchanged.
    fn note<T>(&self, queue: &QueueKey, r: EngineResult<T>) -> EngineResult<T> {
        if matches!(r, Err(EngineError::EpochFenced)) {
            self.invalidate_session(queue);
        }
        r
    }

    /// Who currently owns `queue` (ADR-009 L5). A sole-owner handle always returns [`Ownership::Mine`]; a
    /// coordinated handle resolves the live owner — [`Ownership::Mine`] if it is the active owner,
    /// [`Ownership::Elsewhere`] (the redirect target) for a different live owner, or [`Ownership::Unowned`].
    /// This is a read; it does not register the handle or acquire.
    pub async fn ownership(&self, queue: &QueueKey) -> EngineResult<Ownership> {
        let Coordination::Owner {
            owner_id,
            control_plane,
            control_plane_runtime,
            ..
        } = &self.coordination
        else {
            return Ok(Ownership::Mine { epoch: None });
        };
        let now = self.clock.now();
        let res = if let Some(runtime) = control_plane_runtime {
            runtime.resolve(queue.clone(), now).await?
        } else {
            control_plane.resolve_queue_owner(queue, now)?
        };
        Ok(match res.active_owner {
            Some(o) if &o == owner_id => Ownership::Mine {
                epoch: res.assignment_epoch,
            },
            Some(o) => Ownership::Elsewhere {
                owner: o,
                epoch: res.assignment_epoch,
            },
            None => Ownership::Unowned,
        })
    }

    /// Renew this handle's leases for all queues it currently owns + refresh its heartbeat (coordinated
    /// handles only; sole-owner is a no-op). The host spawns this on a bounded cadence — one call per node,
    /// never one task per queue (ADR-002 density / TD-003 §Queue density). A queue whose renewal is rejected
    /// (the handle was superseded) has its cached session dropped, so its next op re-resolves.
    pub fn renew_owned(&self) -> EngineResult<()> {
        let Coordination::Owner {
            owner_id,
            control_plane,
            control_plane_runtime,
            sessions,
            draining,
        } = &self.coordination
        else {
            return Ok(());
        };
        let now = self.clock.now();
        let owned: Vec<(QueueKey, u64)> = sessions
            .lock()
            .expect("poisoned")
            .iter()
            .map(|(q, s)| (q.clone(), s.lease_epoch))
            .collect();
        let renewals: Vec<fireweed_engine::LeaseRenewal> = owned
            .iter()
            .map(|(queue, lease_epoch)| fireweed_engine::LeaseRenewal {
                queue: queue.clone(),
                owner: owner_id.clone(),
                expected_epoch: *lease_epoch,
            })
            .collect();
        let outcomes = if let Some(runtime) = control_plane_runtime {
            runtime.renew(owner_id.clone(), renewals, now)?
        } else {
            control_plane.heartbeat(owner_id, now)?;
            control_plane.renew_queue_leases(&renewals, now)?
        };
        if outcomes.len() != owned.len() {
            return Err(EngineError::Storage(format!(
                "control-plane batch renewal returned {} outcomes for {} inputs",
                outcomes.len(),
                owned.len()
            )));
        }
        apply_owned_renewal_outcomes(sessions, draining, owned, outcomes)
    }

    /// Whether this owner has observed `queue` as `Draining` (drain split): new claims are refused while
    /// in-flight ops continue. Sole-owner is never draining.
    fn is_draining(&self, queue: &QueueKey) -> bool {
        match &self.coordination {
            Coordination::Owner { draining, .. } => {
                draining.lock().expect("poisoned").contains(queue)
            }
            Coordination::Sole => false,
        }
    }

    pub async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        self.backend.create_queue(definition).await
    }

    /// Read the queue's persisted definition without retaining the result of queue creation. This is
    /// read-only and is useful to obtain queue-local policy such as `progress_bound_ms` for advisory
    /// routing helpers.
    pub async fn queue_definition(&self, queue: &QueueKey) -> EngineResult<QueueDefinition> {
        self.backend.queue_definition(queue).await
    }

    /// Atomically create a queue from `template`, or prove that its stored definition is an exact match.
    ///
    /// This operation delegates create-or-read arbitration to the backend and compares the entire returned
    /// effective definition. It is never called implicitly by data-plane operations.
    pub async fn ensure_queue(
        &self,
        queue: &QueueKey,
        template: &QueueTemplate,
    ) -> Result<EnsureQueueOutcome, EnsureQueueError> {
        let template_name = template.template_name.clone();
        let template_revision = template.template_revision.clone();
        let desired = template
            .resolve(queue)
            .map_err(|error| EnsureQueueError::Validation {
                error,
                template_name: template_name.clone(),
                template_revision: template_revision.clone(),
            })?;

        let outcome = match self.backend.create_queue(desired.clone()).await {
            Ok(outcome) => outcome,
            Err(EngineError::QueueDefinitionConflict) => {
                let stored = self
                    .backend
                    .queue_definition(queue)
                    .await
                    .map_err(|error| EnsureQueueError::Backend {
                        error,
                        template_name: template_name.clone(),
                        template_revision: template_revision.clone(),
                    })?;
                return Err(EnsureQueueError::DefinitionConflict {
                    created: false,
                    desired: Box::new(desired),
                    stored: Box::new(stored),
                    template_name,
                    template_revision,
                });
            }
            Err(error) => {
                return Err(EnsureQueueError::Backend {
                    error,
                    template_name,
                    template_revision,
                });
            }
        };

        if desired != outcome.definition {
            return Err(EnsureQueueError::DefinitionConflict {
                created: outcome.created,
                desired: Box::new(desired),
                stored: Box::new(outcome.definition),
                template_name,
                template_revision,
            });
        }
        Ok(EnsureQueueOutcome {
            created: outcome.created,
            definition: outcome.definition,
            template_name,
            template_revision,
        })
    }

    /// Enqueue one new item (append). Routes through `PushPort`, so the backend assigns a unique,
    /// restart-safe id and commits through its divergence-safe UoW. Returns the server-assigned id.
    pub async fn push(&self, queue: &QueueKey, item: NewItem) -> EngineResult<ItemId> {
        let ids = self.push_batch(queue, vec![item]).await?;
        Ok(ids.into_iter().next().expect("one id per pushed item"))
    }

    /// Enqueue one item under an API-001 request id. Replaying the same request body with the same
    /// `request_id` returns the original item id on backends that implement durable request replay.
    ///
    /// See [`Self::push_batch_with_request_id`] for replay-vs-fresh disposition.
    pub async fn push_with_request_id(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
        item: NewItem,
    ) -> EngineResult<(ItemId, PushDisposition)> {
        let outcome = self
            .push_batch_with_request_id(queue, request_id, vec![item])
            .await?;
        let id = outcome
            .item_ids
            .into_iter()
            .next()
            .expect("one id per pushed item");
        Ok((id, outcome.disposition))
    }

    /// Enqueue a batch of new items in one command (append). Returns the server-assigned ids in order.
    pub async fn push_batch(
        &self,
        queue: &QueueKey,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        let definition = self.backend.queue_definition(queue).await?;
        if items.len() > definition.max_push_batch_size as usize {
            return Err(EngineError::BatchTooLarge);
        }
        let specs: Vec<PushSpec> = items.into_iter().map(new_item_to_spec).collect();
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self.backend.push(queue, specs, now, epoch).await;
        self.note(queue, r)
    }

    /// Enqueue a batch under an API-001 request id.
    ///
    /// Replaying the same batch body with the same request id returns the original ids and
    /// [`PushDisposition::Replayed`]. A first application returns [`PushDisposition::Fresh`].
    /// A different body for the same request id returns `RequestIdConflict`.
    pub async fn push_batch_with_request_id(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
        items: Vec<NewItem>,
    ) -> EngineResult<PushBatchOutcome> {
        let definition = self.backend.queue_definition(queue).await?;
        if items.len() > definition.max_push_batch_size as usize {
            return Err(EngineError::BatchTooLarge);
        }
        let specs: Vec<PushSpec> = items.into_iter().map(new_item_to_spec).collect();
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self
            .backend
            .push_with_request_id(queue, request_id, specs, now, epoch)
            .await;
        self.note(queue, r)
    }

    /// Upsert on a caller-supplied `client_item_key` (Invariant 2). Replaces a pending item with the
    /// same key through the active composition's authoritative mutation boundary.
    pub async fn upsert(
        &self,
        queue: &QueueKey,
        client_item_key: ClientItemKey,
        item: NewItem,
    ) -> EngineResult<UpsertOutcome> {
        let epoch = self.session_epoch(queue).await?;
        let r = self
            .backend
            .replace_if_pending(
                queue,
                &client_item_key,
                item.priority,
                item.group_key,
                item.not_before,
                item.payload,
                item.fields,
                item.metadata,
                item.entity,
                self.clock.now(),
                epoch,
            )
            .await;
        self.note(queue, r)
    }

    /// Claim up to `max` eligible items in priority order, leasing them for `lease_ms` from now.
    /// Item-level claim (no compatibility options).
    pub async fn claim(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
    ) -> EngineResult<Vec<ClaimedItem>> {
        self.claim_with(queue, max, lease_ms, ClaimCompatibility::default())
            .await
    }

    /// Claim with API-001 compatibility options (group_batching / whole_cohort / same_group_key /
    /// group_key / metadata_equals). `ClaimCompatibility::default()` is the item-level claim (see
    /// [`claim`](Self::claim)). Every supported composition implements each declared claim unit without
    /// silently downgrading to item-level delivery.
    pub async fn claim_with(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> EngineResult<Vec<ClaimedItem>> {
        Ok(self
            .claim_response_with(queue, max, lease_ms, compatibility)
            .await?
            .items)
    }

    /// Claim with API-001 compatibility options and return the full response envelope. Use this when the
    /// caller needs top-level fields such as `cohort_lease_token` for `whole_cohort` claims.
    pub async fn claim_response_with(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> EngineResult<Claimed> {
        self.claim_response_at(
            queue,
            ClaimAt::new(max, lease_ms).compatibility(compatibility),
        )
        .await
    }

    /// Claim at caller-resolved times: select the work due at [`ClaimAt::eligibility_time`] while the
    /// lease runs from [`ClaimAt::lease_time`] (defaulting to this handle's clock). This is the claim to
    /// use for SCHEDULED work — a scheduler tick / backfill / replay resolves the execution epoch it is
    /// selecting for, and the leases it takes out stay valid against the operational clock. With neither
    /// time set it is exactly [`claim_with`](Self::claim_with).
    pub async fn claim_at(
        &self,
        queue: &QueueKey,
        request: ClaimAt,
    ) -> EngineResult<Vec<ClaimedItem>> {
        Ok(self.claim_response_at(queue, request).await?.items)
    }

    /// [`claim_at`](Self::claim_at) returning the full response envelope (`cohort_lease_token` and friends),
    /// as [`claim_response_with`](Self::claim_response_with) is to [`claim_with`](Self::claim_with).
    pub async fn claim_response_at(
        &self,
        queue: &QueueKey,
        request: ClaimAt,
    ) -> EngineResult<Claimed> {
        let definition = self.backend.queue_definition(queue).await?;
        if request.max > definition.max_claim_batch_size as usize {
            return Err(EngineError::BatchTooLarge);
        }
        // Drain split (TD-003 §Graceful Drain): a draining owner refuses a NEW claim with a retryable
        // `Unavailable` so in-flight leases finalize before handoff; pushes/finalizes/renews continue.
        if self.is_draining(queue) {
            return Err(EngineError::Unavailable);
        }
        let expected_epoch = self.session_epoch(queue).await?;
        // The two times a claim needs, resolved independently. `lease_time` is operational: it stamps the
        // command and anchors the lease expiry, so it defaults to this handle's clock (never to the
        // eligibility epoch — a claim selecting last hour's due work must not hand out an already-expired
        // lease). `eligibility_time` only selects; unset, it collapses to the operational time, which is the
        // single-clock behaviour of `claim`/`claim_with`.
        let lease_time = request.lease_time.unwrap_or_else(|| self.clock.now());
        let n = self.next();
        let req = ClaimRequest {
            shard: queue.clone(),
            worker_id: WorkerId::new("lib").expect("w"),
            max_items: request.max,
            lease_token: LeaseToken::new(format!("libL{n}")).expect("lease"),
            lease_expires_at: add_millis(lease_time, request.lease_ms),
            now: lease_time,
            eligibility_time: request.eligibility_time,
            compatibility: request.compatibility,
            // Sole-owner: None (never fences). Coordinated owner: the cached acquire-time fence epoch.
            expected_epoch,
        };
        let r = self.backend.claim(req).await;
        self.note(queue, r)
    }

    /// Claim independently from several queues after a shared, no-effect preflight.
    ///
    /// Structural and queue-definition errors fail the outer result before ownership is acquired or any
    /// claim is submitted. Coordinated ownership is then acquired in lexical `(tenant_id, queue_id)` order,
    /// while returned entries remain correlated in caller input order. Every target uses one common lease
    /// instant; an unset eligibility instant resolves to that same snapshot.
    ///
    /// This operation is deliberately **not atomic across queues**. Once dispatch starts, every target runs
    /// independently and its success or failure appears in [`MultiQueueClaimResult::result`]. Dropping the
    /// outer future cancels waiting, not committed work: in particular, an admitted durable backend call may
    /// still complete. For a single-queue atomic transition that consumes several existing claims, use
    /// [`Fireweed::commit_multi_claim`] instead.
    pub async fn claim_across_queues(
        &self,
        targets: Vec<MultiQueueClaimTarget>,
        limits: MultiQueueClaimLimits,
    ) -> EngineResult<Vec<MultiQueueClaimResult>> {
        if targets.is_empty() {
            return Err(EngineError::Invalid(
                "multi-queue claim targets must not be empty",
            ));
        }
        if limits.max_targets == 0 {
            return Err(EngineError::Invalid(
                "multi-queue claim max_targets must be positive",
            ));
        }
        if limits.max_total_items == 0 {
            return Err(EngineError::Invalid(
                "multi-queue claim max_total_items must be positive",
            ));
        }
        if limits.max_targets > MAX_MULTI_QUEUE_CLAIM_TARGETS {
            return Err(EngineError::Invalid(
                "multi-queue claim max_targets exceeds fixed ceiling",
            ));
        }
        if limits.max_total_items > MAX_MULTI_QUEUE_CLAIM_ITEMS {
            return Err(EngineError::Invalid(
                "multi-queue claim max_total_items exceeds fixed ceiling",
            ));
        }
        if targets.len() > MAX_MULTI_QUEUE_CLAIM_TARGETS {
            return Err(EngineError::Invalid(
                "multi-queue claim target count exceeds fixed ceiling",
            ));
        }
        if targets.len() > limits.max_targets {
            return Err(EngineError::Invalid(
                "multi-queue claim target count exceeds caller limit",
            ));
        }

        let mut queues = HashSet::with_capacity(targets.len());
        let mut total_items = 0usize;
        for target in &targets {
            if target.claim.max == 0 {
                return Err(EngineError::Invalid(
                    "multi-queue claim target max must be positive",
                ));
            }
            if !queues.insert(target.queue.clone()) {
                return Err(EngineError::Invalid(
                    "multi-queue claim contains a duplicate queue",
                ));
            }
            if target.claim.lease_time.is_some() {
                return Err(EngineError::Invalid(
                    "multi-queue claim target lease_time must be unset",
                ));
            }
            total_items = total_items
                .checked_add(target.claim.max)
                .ok_or(EngineError::Invalid(
                    "multi-queue claim aggregate items exceed fixed ceiling",
                ))?;
        }
        if total_items > MAX_MULTI_QUEUE_CLAIM_ITEMS {
            return Err(EngineError::Invalid(
                "multi-queue claim aggregate items exceed fixed ceiling",
            ));
        }
        if total_items > limits.max_total_items {
            return Err(EngineError::Invalid(
                "multi-queue claim aggregate items exceed caller limit",
            ));
        }

        // Load every definition before validating any target. This keeps missing-definition and
        // compatibility failures entirely ahead of coordinated ownership acquisition.
        let mut definitions = Vec::with_capacity(targets.len());
        for target in &targets {
            definitions.push(self.backend.queue_definition(&target.queue).await?);
        }
        for (target, definition) in targets.iter().zip(&definitions) {
            if target.claim.max > definition.max_claim_batch_size as usize {
                return Err(EngineError::BatchTooLarge);
            }
            validate_claim_compatibility(
                &target.claim.compatibility,
                target.claim.max as u64,
                definition,
            )?;
        }

        let common_time = self.clock.now();
        let mut ownership_order: Vec<&QueueKey> =
            targets.iter().map(|target| &target.queue).collect();
        ownership_order.sort_by(|left, right| {
            left.tenant_id
                .as_str()
                .cmp(right.tenant_id.as_str())
                .then_with(|| left.queue_id.as_str().cmp(right.queue_id.as_str()))
        });
        for queue in ownership_order {
            self.session_epoch_at(queue, common_time).await?;
        }

        let claims = targets.into_iter().map(|target| async move {
            let queue = target.queue;
            let mut claim = target.claim;
            claim.lease_time = Some(common_time);
            if claim.eligibility_time.is_none() {
                claim.eligibility_time = Some(common_time);
            }
            let result = self.claim_response_at(&queue, claim).await;
            MultiQueueClaimResult { queue, result }
        });
        Ok(futures::future::join_all(claims).await)
    }

    /// Complete (ack) the given leased items. All-or-nothing (a fenced/superseded/non-leased id rejects
    /// the batch with the structured error, committing nothing).
    pub async fn ack(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Complete, None)
            .await
    }

    /// Complete a batch of leased items. This is the worker-loop alias for [`Self::ack`] and preserves its
    /// all-or-nothing batch transition and structured errors. Existing `ack` callers remain supported.
    pub async fn complete(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.ack(queue, ids).await
    }

    /// Return leased items to the queue: `Retry` (optionally with a backoff `not_before`) or `Release`.
    pub async fn nack(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        how: Nack,
    ) -> EngineResult<()> {
        let (kind, not_before) = match how {
            Nack::Retry { not_before } => (FinalizeKind::Retry, not_before),
            Nack::Release => (FinalizeKind::Release, None),
        };
        self.finalize(queue, ids, kind, not_before).await
    }

    /// Return a batch of leased items for another attempt, optionally deferred until the absolute
    /// `not_before` timestamp. This is the worker-loop alias for `nack(..., Nack::Retry { not_before })` and
    /// preserves its all-or-nothing batch transition, attempt accounting, retry exhaustion, and errors.
    /// Pass `None` for an immediate retry or use [`Self::retry_after`] for a relative delay.
    pub async fn retry(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        not_before: Option<UtcTimestamp>,
    ) -> EngineResult<()> {
        self.nack(queue, ids, Nack::Retry { not_before }).await
    }

    /// Release a batch of leased items immediately back to pending work. This is the worker-loop alias for
    /// `nack(..., Nack::Release)` and preserves its all-or-nothing transition and structured errors.
    pub async fn release(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.nack(queue, ids, Nack::Release).await
    }

    /// `nack(Retry)` with a **relative** backoff: defer the item's re-eligibility by `delay_ms` from now
    /// (queue-native retry backoff, computed off this handle's clock).
    pub async fn nack_retry_after(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        delay_ms: u64,
    ) -> EngineResult<()> {
        let not_before = Some(add_millis(self.clock.now(), delay_ms));
        self.nack(queue, ids, Nack::Retry { not_before }).await
    }

    /// Retry a batch after a relative delay from this handle's clock. This is the worker-loop alias for
    /// [`Self::nack_retry_after`] and preserves the same all-or-nothing transition and retry timing.
    pub async fn retry_after(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        delay_ms: u64,
    ) -> EngineResult<()> {
        self.nack_retry_after(queue, ids, delay_ms).await
    }

    async fn finalize(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        kind: FinalizeKind,
        not_before: Option<UtcTimestamp>,
    ) -> EngineResult<()> {
        let outcomes: Vec<FinalizeOutcome> = ids
            .into_iter()
            .map(|item_id| FinalizeOutcome {
                item_id,
                kind,
                applied_state: None,
                not_before,
            })
            .collect();
        let epoch = self.session_epoch(queue).await?;
        let r = self
            .backend
            .finalize(queue, outcomes, self.clock.now(), epoch)
            .await;
        self.note(queue, r)
    }

    /// Authoritative vectorized single-claim commit (Snorri StateStore boundary, epic pqueue-2201fd37).
    /// Each [`CommitEntry`] is ONE recoverable transition: it validates a lease-token + version-fenced
    /// [`ClaimRef`], writes opaque non-work `side_records` (authoritative workflow state/audit that is NOT
    /// claimable work), enqueues `lifecycle_items` as ordinary dispatchable work (outbox/await/timer), and
    /// finalizes the input claim — atomically per entry. `request_id` gives the whole body retained
    /// replay/conflict/expired semantics, so a retried transition returns the prior outcomes without
    /// double-writing. Per-entry [`EntryOutcome`]s are independent (all-or-nothing is NOT required across
    /// entries). Use [`Fireweed::commit_multi_claim`] when one entry must consume multiple claims.
    pub async fn commit(
        &self,
        queue: &QueueKey,
        request: CommitRequest,
    ) -> EngineResult<Vec<EntryOutcome>> {
        let CommitRequest {
            request_id,
            entries,
        } = request;
        let entries: Vec<CommitTransitionEntry> = entries
            .into_iter()
            .map(|e| CommitTransitionEntry {
                claim_ref: e.claim_ref,
                additional_claim_refs: Vec::new(),
                finalize: e.finalize,
                side_records: e.side_records,
                lifecycle_items: e
                    .lifecycle_items
                    .into_iter()
                    .map(new_item_to_spec)
                    .collect(),
                instance_fence: e.instance_fence,
            })
            .collect();
        let transition = CommitTransition {
            request_id,
            entries,
        };
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self
            .backend
            .commit_transition(queue, transition, now, epoch)
            .await;
        let outcomes = self.note(queue, r)?;
        Ok(outcomes
            .into_iter()
            .map(|o| match o {
                CommitEntryOutcome::Committed { lifecycle_item_ids } => {
                    EntryOutcome::Committed { lifecycle_item_ids }
                }
                CommitEntryOutcome::Rejected(e) => EntryOutcome::Rejected(e),
            })
            .collect())
    }

    /// Commit entries that each finalize a primary claim plus zero or more additional claims as one atomic
    /// transition. This preserves the scalar [`CommitEntry`] surface while supporting workflow boundaries
    /// that must consume a result and its matching await before appending a continuation.
    pub async fn commit_multi_claim(
        &self,
        queue: &QueueKey,
        request: MultiClaimCommitRequest,
    ) -> EngineResult<Vec<EntryOutcome>> {
        let MultiClaimCommitRequest {
            request_id,
            entries,
        } = request;
        let entries = entries
            .into_iter()
            .map(|entry| CommitTransitionEntry {
                claim_ref: entry.claim_ref,
                additional_claim_refs: entry.additional_claim_refs,
                finalize: entry.finalize,
                side_records: entry.side_records,
                lifecycle_items: entry
                    .lifecycle_items
                    .into_iter()
                    .map(new_item_to_spec)
                    .collect(),
                instance_fence: entry.instance_fence,
            })
            .collect();
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let outcomes = self
            .note(
                queue,
                self.backend
                    .commit_transition(
                        queue,
                        CommitTransition {
                            request_id,
                            entries,
                        },
                        now,
                        epoch,
                    )
                    .await,
            )?
            .into_iter()
            .map(|outcome| match outcome {
                CommitEntryOutcome::Committed { lifecycle_item_ids } => {
                    EntryOutcome::Committed { lifecycle_item_ids }
                }
                CommitEntryOutcome::Rejected(error) => EntryOutcome::Rejected(error),
            })
            .collect();
        Ok(outcomes)
    }

    /// The backend's authoritative-commit capability descriptors (epic pqueue-2201fd37, ADR-009). A consumer
    /// (Snorri) reads these BEFORE activation and rejects a backend that does not advertise the guarantees it
    /// needs (e.g. `atomic_transition_commit`). The composed backends derive these descriptors from
    /// the authoritative object log and the projection's transition support. `queue` is accepted for
    /// signature stability — the capability set is backend-wide.
    pub fn commit_capabilities(&self, _queue: &QueueKey) -> EngineResult<CommitCapabilities> {
        Ok(self.backend.commit_capabilities())
    }

    /// Recovery/explain read for a committed transition (epic pqueue-2201fd37 acceptance #5). Reconstructs the
    /// transition addressed by `request_id` — the consumed input id, the advanced instance fence, the
    /// side-record keys, the lifecycle item ids, and per-entry status — from the retained commit idempotency
    /// record plus current durable state. `Ok(None)` when no such record is retained. Proves committed
    /// state/audit remains recoverable after the input is finalized.
    pub async fn explain_commit(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
    ) -> EngineResult<Option<CommitRecovery>> {
        let r = self.backend.explain_commit(queue, request_id).await;
        self.note(queue, r)
    }

    /// Read one opaque non-work side record by key (epic pqueue-2201fd37 acceptance #5). Side records are
    /// disjoint from work items, so this never reflects claimable work and survives input finalization.
    /// `Ok(None)` if unwritten.
    pub async fn side_record(&self, queue: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        let r = self.backend.side_record(queue, key).await;
        self.note(queue, r)
    }

    /// Non-destructive priority-ordered view of eligible items.
    pub async fn peek(&self, queue: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        self.backend.peek(queue, limit).await
    }

    /// The current durable command position for `queue` (thin wrapper over `high_water`).
    pub async fn current_position(&self, queue: &QueueKey) -> EngineResult<CommandPosition> {
        self.backend.current_position(queue).await
    }

    /// Reconstruct `queue`'s projection as of `position`, run `query` against it, and discard it.
    #[allow(dead_code)] // Held until the backend-neutral history component replaces this private seam.
    pub async fn read_as_of<T, F>(
        &self,
        queue: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&<B as HistoricalProjectionRead>::AsOfProjection) -> EngineResult<T>
            + Send
            + 'static,
    {
        self.backend.read_as_of(queue, position, query).await
    }

    /// Discover `queue`'s **active scopes** — the scopes (the queue rolled up, or its per-group detail at
    /// [`DiscoveryGranularity::Group`]) that currently hold eligible work — ranked **oldest-eligible first**
    /// (the most-starved scope leads; deterministic group-key tiebreak). Each [`ActiveScope`] carries the
    /// scope's TRUE `oldest_eligible_age_ms` (age from `now`) and eligible count, so a worker can route to
    /// the most-aged work and a stalled queue (eligible work piling up with nothing claiming it) is visible
    /// as a growing `oldest_eligible_age_ms` even with no live serving owner (FR-41). The queue has one owner
    /// (ADR-008), so this owner-local ranking is authoritative for the queue without a cross-owner merge.
    ///
    /// AUTHZ (ADR-002): this trusted library facade has **no principal** — it returns the UNFILTERED ranked
    /// discovery for the addressed queue. Excluding scopes a caller is not authorized to see is the
    /// **auth layer's** concern (the RESP/server front stamps the principal and filters); it is deliberately
    /// not invented here. Every projection axis maintains the exact grouping, eligibility timestamp, and
    /// gate state required for this result.
    pub async fn discover_active_scopes(
        &self,
        queue: &QueueKey,
        granularity: DiscoveryGranularity,
    ) -> EngineResult<Vec<ActiveScope>> {
        let now = self.clock.now();
        self.backend
            .discover_active_scopes(queue, granularity, now)
            .await
    }

    /// Discover active scopes while retaining the exact request coordinates alongside the unchanged
    /// backend result. This accessor does not filter or re-rank [`Self::discover_active_scopes`].
    pub async fn discover_active_scopes_stamped(
        &self,
        queue: &QueueKey,
        granularity: DiscoveryGranularity,
    ) -> EngineResult<ActiveScopeDiscovery> {
        let scopes = self.discover_active_scopes(queue, granularity).await?;
        Ok(ActiveScopeDiscovery {
            queue: queue.clone(),
            granularity,
            scopes,
        })
    }

    /// Discover eligible scopes in the backend's authoritative oldest-eligible-first order. This is a
    /// read-only alias for [`Self::discover_active_scopes`]; it neither filters, re-ranks, nor reserves work.
    pub async fn discover(
        &self,
        queue: &QueueKey,
        granularity: DiscoveryGranularity,
    ) -> EngineResult<Vec<ActiveScope>> {
        self.discover_active_scopes(queue, granularity).await
    }

    /// Read one live hot-storage item by caller-supplied key. Returns `None` once the item is complete,
    /// failed, purged, or superseded; leased items still count as live work and are returned.
    pub async fn live_item(
        &self,
        queue: &QueueKey,
        key: ClientItemKey,
    ) -> EngineResult<Option<LiveItemView>> {
        Ok(self
            .backend
            .live_items(queue, &[key])
            .await?
            .into_iter()
            .next()
            .unwrap_or(None))
    }

    /// Read live hot-storage items by caller-supplied key, preserving input order.
    pub async fn live_items(
        &self,
        queue: &QueueKey,
        keys: Vec<ClientItemKey>,
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        self.backend.live_items(queue, &keys).await
    }

    /// Exact composite-key get on a UNIQUE secondary index (ADR-010). `key` is the per-field value bytes
    /// in the index's declared field order. Returns the single [`IndexHit`] holding the key, or `None`.
    /// Pure read (no epoch/fence). `EngineError::Invalid` if `index` is not a unique index on this queue.
    ///
    /// For typed (ADR-011 [`QueueIndex`]) indexes, prefer [`Fireweed::query_index_unique_typed`] — it
    /// accepts [`serde_json::Value`]s directly and validates the type encoding at the API boundary.
    /// This raw-byte overload remains available for legacy [`IndexSpec`] indexes. For typed indexes,
    /// passing bytes that do not decode to the declared field type returns [`EngineError::Invalid`].
    pub async fn query_index_unique(
        &self,
        queue: &QueueKey,
        index: &str,
        key: Vec<Vec<u8>>,
    ) -> EngineResult<Option<IndexHit>> {
        self.backend.index_get_unique(queue, index, &key).await
    }

    /// Exact composite-key lookup on a secondary index (unique or non-unique, ADR-010). Returns every
    /// matching item ordered by `item_id` ascending. Pure read (no epoch/fence).
    ///
    /// For typed (ADR-011 [`QueueIndex`]) indexes, prefer [`Fireweed::query_index_typed`] — it accepts
    /// [`serde_json::Value`]s directly and validates the type encoding at the API boundary. This
    /// raw-byte overload remains available for legacy [`IndexSpec`] indexes. For typed indexes,
    /// passing bytes that do not decode to the declared field type returns [`EngineError::Invalid`].
    pub async fn query_index(
        &self,
        queue: &QueueKey,
        index: &str,
        key: Vec<Vec<u8>>,
    ) -> EngineResult<Vec<IndexHit>> {
        self.backend.index_lookup(queue, index, &key).await
    }

    /// Typed composite-key get on a UNIQUE secondary index (ADR-011). `key_values` are the per-field
    /// [`serde_json::Value`]s in the [`QueueIndex`] declaration order — the index name must match the
    /// configured [`QueueIndex::name`] exactly. Each value is validated against the declared field type
    /// with the same ESF encoder used by the projection, then serialized to the axon_esf-compatible byte
    /// format (strings/datetimes as raw UTF-8; numbers and booleans as JSON bytes) before lookup; a
    /// wrong-type value returns [`EngineError::Invalid`].
    /// Pure read (no epoch/fence). `EngineError::Invalid` for an unknown index name, non-unique index, or
    /// arity mismatch.
    pub async fn query_index_unique_typed(
        &self,
        queue: &QueueKey,
        index: &str,
        key_values: &[serde_json::Value],
    ) -> EngineResult<Option<IndexHit>> {
        let definition = self.backend.queue_definition(queue).await?;
        let spec = definition
            .typed_indexes
            .iter()
            .find(|spec| spec.name == index)
            .ok_or(EngineError::Invalid("unknown secondary index"))?;
        if !typed_index_unique(spec) {
            return Err(EngineError::Invalid("secondary index is not unique"));
        }
        let raw = typed_index_query_key_bytes(spec, key_values)?;
        self.backend.index_get_unique(queue, index, &raw).await
    }

    /// Typed composite-key lookup on a secondary index — unique or non-unique (ADR-011). `key_values`
    /// are the per-field [`serde_json::Value`]s in the [`QueueIndex`] declaration order; the index name
    /// must match the configured [`QueueIndex::name`] exactly. Returns every matching item ordered by
    /// `item_id` ascending; empty if none. Each value is validated against the declared field type with
    /// the same ESF encoder used by the projection, then serialized to the axon_esf-compatible byte
    /// format before lookup — a wrong-type value returns [`EngineError::Invalid`].
    /// Pure read (no epoch/fence).
    pub async fn query_index_typed(
        &self,
        queue: &QueueKey,
        index: &str,
        key_values: &[serde_json::Value],
    ) -> EngineResult<Vec<IndexHit>> {
        let definition = self.backend.queue_definition(queue).await?;
        let spec = definition
            .typed_indexes
            .iter()
            .find(|spec| spec.name == index)
            .ok_or(EngineError::Invalid("unknown secondary index"))?;
        let raw = typed_index_query_key_bytes(spec, key_values)?;
        self.backend.index_lookup(queue, index, &raw).await
    }

    /// Dead-letter (terminal `fail`) the given leased items.
    pub async fn fail(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Fail, None).await
    }

    /// Per-state counts for the queue.
    pub async fn metrics(&self, queue: &QueueKey) -> EngineResult<QueueMetrics> {
        self.backend.metrics(queue).await
    }

    /// Exact Pending/Leased/Complete/Failed counts restricted by filters over one declared typed index.
    /// This is a read-only projection query and never claims or mutates matching rows.
    pub async fn metrics_by_query(
        &self,
        queue: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        self.backend.metrics_by_query(queue, request).await
    }

    /// Extend the lease on the given in-flight items to `lease_ms` from now — a long-running worker keeps
    /// its claim WITHOUT a re-delivery (`attempt_count` unchanged). Pre-validated: a fenced/superseded/
    /// terminal/non-leased id rejects the batch with the structured error, committing nothing.
    pub async fn renew(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        lease_ms: u64,
    ) -> EngineResult<()> {
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let ids: Vec<ItemId> = ids.into_iter().collect();
        let r = self
            .backend
            .renew(queue, ids, add_millis(now, lease_ms), now, epoch)
            .await;
        self.note(queue, r)
    }

    /// Transfer the given in-flight items to a FRESH lease (a re-delivery to a new worker — charges one
    /// attempt, per the delivery-count invariant), leasing them for `lease_ms` from now. Mints a new
    /// lease token. Pre-validated like [`Fireweed::renew`].
    pub async fn reassign(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        lease_ms: u64,
    ) -> EngineResult<()> {
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let n = self.next();
        let token = LeaseToken::new(format!("libL{n}")).expect("lease");
        let ids: Vec<ItemId> = ids.into_iter().collect();
        let r = self
            .backend
            .reassign(queue, ids, token, add_millis(now, lease_ms), now, epoch)
            .await;
        self.note(queue, r)
    }

    /// In-place merge of a **live** item's hot-storage `fields`/`payload` (FAC-1) — the write half of the
    /// [`live_item`](Self::live_item) map, so an owner-runtime can keep compound per-item work state in
    /// Fireweed instead of a side shadow store. Field names reserved by API-001 are rejected before the
    /// backend sees the write; legal only while the item is Pending OR Leased; touches neither lifecycle
    /// state nor the lease. `field_ops`: `Some(bytes)` sets/overwrites a key, `None` removes it.
    /// `payload`: [`PayloadUpdate::Keep`] leaves the body, `Set(_)` replaces (`Set(None)` clears).
    /// `expected_item_version`: optional CAS — a mismatch rejects with [`EngineError::Conflict`] and commits
    /// nothing (for rolling concurrent updates). Bumps and returns the new `item_version`. Fenced by the
    /// owner's epoch and recorded in the authoritative log when the projection is rebuildable.
    pub async fn update_fields(
        &self,
        queue: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
    ) -> EngineResult<u64> {
        validate_api001_reserved_write_fields(&field_ops)?;
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self
            .backend
            .update_fields(
                queue,
                item_id,
                field_ops,
                payload,
                entity,
                expected_item_version,
                now,
                epoch,
            )
            .await;
        self.note(queue, r)
    }

    /// Replace mutable fields on one or more pending, non-leased items using the full API-001
    /// `BatchUpdate` contract. The backend executes the request as one batch and returns one outcome
    /// per entry in request order. `request_id` makes response-loss retries converge on the original
    /// committed results; reusing it with a different body returns
    /// [`EngineError::RequestIdConflict`].
    ///
    /// Each [`BatchUpdateValue::Keep`] leaves the stored value unchanged; `Replace` performs full
    /// replacement. Entry-local validation failures return [`BatchUpdateOutcome::Invalid`] without
    /// aborting valid siblings, leased entries return `Conflict`, terminal entries return `Terminal`, and
    /// successful entries bump `item_version` while preserving `eligible_since`.
    pub async fn batch_update(
        &self,
        queue: &QueueKey,
        request: BatchUpdateRequest,
    ) -> EngineResult<BatchUpdateResponse>
    where
        B: BatchUpdatePort,
    {
        if request.updates.is_empty() {
            return Err(EngineError::Invalid("empty batch update"));
        }
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let result = self.backend.batch_update(queue, request, now, epoch).await;
        self.note(queue, result)
    }

    /// Atomically mutate addressed items or the first matching selector clause through the mandatory,
    /// backend-erased Fireweed contract. Selector resolution, lease/version/predicate checks, item patches,
    /// and queue gate changes share one queue-local durable command.
    pub async fn mutate_items(
        &self,
        queue: &QueueKey,
        request: ItemMutationRequest,
    ) -> EngineResult<ItemMutationResponse>
    where
        B: ItemMutationPort,
    {
        let epoch = self.session_epoch(queue).await?;
        let result = self.backend.mutate_items(queue, request, epoch).await;
        self.note(queue, result)
    }

    /// Reschedule a **live** item's `priority` and/or `not_before` after push (BQ pqueue-7a96f929) — the
    /// "change when/where this item is delivered" verb, distinct from [`Fireweed::update_fields`] (which merges
    /// hot-storage fields/payload). [`ScheduleUpdate::Keep`] leaves a dimension unchanged; `Set(Some(v))`
    /// sets it; `Set(None)` clears it (clearing `not_before` makes the item immediately eligible; clearing
    /// `priority` drops it to the unpriced FIFO tail). A priority change re-keys the item in the eligibility
    /// order; a `not_before` change re-gates its eligibility (so a deferred item leaves the claimable set
    /// until its new time). Legal while the item is Pending OR Leased; pre-validated like `update_fields`
    /// (absent/terminal/superseded id → reject; `expected_item_version` mismatch → [`EngineError::Conflict`]),
    /// fenced by the owner's epoch. Bumps and returns the new `item_version`.
    pub async fn update(
        &self,
        queue: &QueueKey,
        item_id: ItemId,
        priority: ScheduleUpdate<PriorityValue>,
        not_before: ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
    ) -> EngineResult<u64> {
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self
            .backend
            .reschedule(
                queue,
                item_id,
                priority,
                not_before,
                expected_item_version,
                now,
                epoch,
            )
            .await;
        self.note(queue, r)
    }

    /// Block or unblock the given gate keys for `queue` (BQ-14d, API-001 g2 `SetGates`). Blocking a gate
    /// key makes every item carrying it INELIGIBLE — a blocked-gated item is never claimed until the key is
    /// unblocked (the relational eligibility predicate anti-joins item gate keys against the queue's gate
    /// state); `blocked = false` restores eligibility. Operator-driven (drains/holds a class of work).
    ///
    /// Every supported projection axis persists or reconstructs gate state and membership. The mutation is
    /// fenced by the owner's epoch.
    pub async fn set_gates(
        &self,
        queue: &QueueKey,
        gate_keys: Vec<String>,
        blocked: bool,
    ) -> EngineResult<()> {
        let epoch = self.session_epoch(queue).await?;
        let r = self
            .backend
            .set_gates(
                queue,
                SetGatesCommand { gate_keys, blocked },
                self.clock.now(),
                epoch,
            )
            .await;
        self.note(queue, r)
    }

    /// Reclaim THIS queue's expired leases (Leased → Pending) under the owner's fence, returning the
    /// reclaimed ids (FAC-2). The host-driven, per-queue equivalent of the background reclaim tick: call it
    /// before a claim on a queue you own to recover orphaned leases on a quiet queue without running the
    /// global sweep. `limit` caps the batch (`None` = all currently expired). Idempotent.
    pub async fn reclaim_expired(
        &self,
        queue: &QueueKey,
        limit: Option<usize>,
    ) -> EngineResult<Vec<ItemId>> {
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self.backend.reclaim_expired(queue, limit, now, epoch).await;
        self.note(queue, r)
    }

    /// [`Fireweed::reclaim_expired`] at a caller-supplied time. This is the deterministic/logical-time
    /// variant for embedders that carry operation time with each request: `now` is forwarded to the
    /// backend without consulting this handle's [`Clock`]. Reclamation semantics, batching, and owner
    /// fencing are otherwise identical to [`Fireweed::reclaim_expired`].
    pub async fn reclaim_expired_at(
        &self,
        queue: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ItemId>> {
        let epoch = self.session_epoch_at(queue, now).await?;
        let r = self.backend.reclaim_expired(queue, limit, now, epoch).await;
        self.note(queue, r)
    }

    /// Re-arm a recurring item: complete this delivery and re-arm it for its next occurrence, RESETTING
    /// `attempt_count` to 0. Maps to `Finalize{Rearm}` with no new `not_before` (re-eligible immediately).
    /// For a recurring item with an idle interval between occurrences use [`Fireweed::rearm_at`].
    pub async fn rearm(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Rearm, None).await
    }

    /// Re-arm a recurring item for its NEXT occurrence at `not_before` (the recurrence interval): completes
    /// this delivery, resets `attempt_count` to 0, and defers re-eligibility until `not_before` — so an idle
    /// recurring item is ineligible (and excluded from oldest-eligible selection) between occurrences. If the
    /// queue's [`RecurrencePolicy::until`] is set and `not_before` falls strictly past it, the series has
    /// ended: the item is driven **terminal** (Complete) instead of re-arming. Maps to `Finalize{Rearm}`
    /// carrying the next-occurrence `not_before`.
    pub async fn rearm_at(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        not_before: UtcTimestamp,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Rearm, Some(not_before))
            .await
    }

    /// [`Fireweed::rearm_at`] with a **relative** interval: re-arm for `delay_ms` from now (the recurrence
    /// period, computed off this handle's clock).
    pub async fn rearm_after(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        delay_ms: u64,
    ) -> EngineResult<()> {
        let not_before = add_millis(self.clock.now(), delay_ms);
        self.rearm_at(queue, ids, not_before).await
    }

    /// Hard-delete the given items (operator purge / dead-letter cleanup). A **leased** item requires
    /// `force` (else `Conflict`); absent ids are no-ops. Returns the count actually removed.
    pub async fn purge(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        force: bool,
    ) -> EngineResult<u64> {
        let epoch = self.session_epoch(queue).await?;
        let ids: Vec<ItemId> = ids.into_iter().collect();
        let r = self
            .backend
            .purge(queue, ids, force, self.clock.now(), epoch)
            .await;
        self.note(queue, r)
    }

    /// Rich view of specific in-flight (leased) items in the claimed-item shape (the read behind RESP
    /// `XCLAIM`'s reply). Ids that are absent or not currently leased are omitted.
    pub async fn claimed(
        &self,
        queue: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<ClaimedItem>> {
        self.backend.claimed_view(queue, ids).await
    }

    /// The hot-projection query capabilities `queue`'s backend advertises (API-004 Query Capability
    /// Names). Every flag defaults to `false` until a backend bead implements the corresponding
    /// capability; a caller MUST check this before issuing [`Fireweed::range_scan`],
    /// [`Fireweed::grouped_aggregate`], [`Fireweed::declared_bucket_segment`],
    /// [`Fireweed::bounded_mutation`], or [`Fireweed::claim_by_query`] rather than discover unavailability
    /// only via the structured [`EngineError::Unavailable`] each returns.
    pub fn hot_projection_capabilities(&self, queue: &QueueKey) -> QueryCapabilityFlags {
        self.backend.hot_projection_capabilities(queue)
    }

    /// Ordered scan over a declared index with cursor pagination (API-004 Range Scan). Returns
    /// [`EngineError::Unavailable`] on a backend that has not implemented `range_scan` — see
    /// [`Fireweed::hot_projection_capabilities`].
    pub async fn range_scan(
        &self,
        queue: &QueueKey,
        request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        self.backend.range_scan(queue, request).await
    }

    /// Grouped/bucketed count aggregation over a declared index (API-004 Grouping / Aggregation).
    /// Returns [`EngineError::Unavailable`] on a backend that has not implemented
    /// `grouped_aggregate` — see [`Fireweed::hot_projection_capabilities`].
    pub async fn grouped_aggregate(
        &self,
        queue: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        self.backend.grouped_aggregate(queue, request).await
    }

    /// Caller-declared numeric bucket segmentation over one declared numeric-indexed field,
    /// including the required null/no-value bucket (API-004 Declared Numeric Buckets). Returns
    /// [`EngineError::Unavailable`] on a backend that has not implemented `declared_bucket_segment`
    /// — see [`Fireweed::hot_projection_capabilities`].
    pub async fn declared_bucket_segment(
        &self,
        queue: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        self.backend.declared_bucket_segment(queue, request).await
    }

    /// Scan a declared-index predicate and apply a caller-specified field update to every matching
    /// record, with per-record optimistic concurrency (API-004 Bounded Mutation). Returns
    /// [`EngineError::Unavailable`] on a backend that has not implemented `bounded_mutation` — see
    /// [`Fireweed::hot_projection_capabilities`].
    pub async fn bounded_mutation(
        &self,
        queue: &QueueKey,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationResponse> {
        let now = self.clock.now();
        let expected_epoch = self.session_epoch_at(queue, now).await?;
        let result = self
            .backend
            .bounded_mutation(
                queue,
                request,
                fireweed_engine::BoundedMutationContext {
                    now,
                    expected_epoch,
                },
            )
            .await;
        self.note(queue, result)
    }

    /// Claim due records selected by a declared-index predicate instead of the queue's default
    /// priority order (API-004 Claim By Query) — an alternate *selection* path into the same claim/
    /// lease/finalize lifecycle as [`Fireweed::claim`], not a parallel one. Returns
    /// [`EngineError::Unavailable`] on a backend that has not implemented `claim_by_query` — see
    /// [`Fireweed::hot_projection_capabilities`].
    pub async fn claim_by_query(
        &self,
        queue: &QueueKey,
        request: ClaimByQueryRequest,
    ) -> EngineResult<Claimed> {
        self.claim_by_query_at(queue, request, ClaimByQueryAt::new())
            .await
    }

    /// Claim by declared-index predicate at caller-resolved times.
    ///
    /// `eligibility_time` decides which scheduled records are due. `lease_time` stamps the lease and
    /// command metadata. Leaving both unset preserves the existing single-clock behavior.
    pub async fn claim_by_query_at(
        &self,
        queue: &QueueKey,
        request: ClaimByQueryRequest,
        at: ClaimByQueryAt,
    ) -> EngineResult<Claimed> {
        if self.is_draining(queue) {
            return Err(EngineError::Unavailable);
        }
        let lease_time = at.lease_time.unwrap_or_else(|| self.clock.now());
        let expected_epoch = self.session_epoch_at(queue, lease_time).await?;
        let result = self
            .backend
            .claim_by_query(
                queue,
                request,
                fireweed_engine::ClaimByQueryContext {
                    now: lease_time,
                    eligibility_time: at.eligibility_time,
                    expected_epoch,
                },
            )
            .await;
        self.note(queue, result)
    }
}

// ---------------------------------------------------------------------------
// Public constructors — the blessed way to build a RuntimeCore WITHOUT naming a backend (ADR-009 §4a / B3).
// The concrete backend is built internally and erased behind `impl LibBackend`, so a client of the
// published crate never holds a port-bearing handle. Reaching a raw port requires deliberately depending
// on an internal crate (strong-by-default, not absolute — OD-6).
// ---------------------------------------------------------------------------

/// Open a **sole-owner**, in-memory Fireweed handle (atomic durability class) — the zero-setup path.
/// Requires the `memory` feature (default).
#[cfg(feature = "memory")]
pub fn open_memory(clock: Arc<dyn Clock>) -> Fireweed {
    Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(fireweed_memory::composed_memory_backend()),
        clock,
    ))
}

/// Open a **sole-owner**, SQLite-backed Fireweed handle with a durable command log and an in-memory
/// projection rebuilt from that log at `path`. Requires the `sqlite` feature (default).
///
/// Matrix cell: `log=sqlite` × `projection=memory` (Class A log-replay).
#[cfg(feature = "sqlite")]
pub fn open_sqlite(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    let backend = Arc::new(fireweed_sqlite::composed_sqlite_backend(path)?);
    Ok(Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(blocking_backend::BlockingLibBackend::new(backend)?),
        clock,
    )))
}

/// Open a **sole-owner** Fireweed handle with a durable sqlite command log at `log_path` and a
/// derived sqlite projection at `projection_path` (Class A; distinct store paths required).
///
/// Matrix cell: `log=sqlite` × `projection=sqlite`. Recovery-on-open replays only the log tail
/// beyond the projection high-water. Requires the `sqlite` feature (default).
#[cfg(feature = "sqlite")]
pub fn open_sqlite_sqlite_projection(
    log_path: &str,
    projection_path: &str,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    if log_path == projection_path {
        return Err(EngineError::Invalid(
            "sqlite×sqlite requires distinct log_path and projection_path",
        ));
    }
    let backend = Arc::new(fireweed_sqlite::composed_sqlite_log_sqlite_projection(
        log_path,
        projection_path,
    )?);
    Ok(Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(blocking_backend::BlockingLibBackend::new(backend)?),
        clock,
    )))
}

/// Open a **sole-owner** Fireweed handle with a durable sqlite command log at `log_path` and a
/// derived postgres relational projection at `projection_url` (Class A; distinct stores).
///
/// Matrix cell: `log=sqlite` × `projection=postgres`. Requires `sqlite` + `postgres` features.
#[cfg(all(feature = "sqlite", feature = "postgres"))]
pub fn open_sqlite_postgres_projection(
    log_path: &str,
    projection_url: &str,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    let log = fireweed_sqlite::SqliteLog::open(log_path)?;
    let projection = fireweed_postgres::PostgresRelational::connect(projection_url)?;
    let backend = Arc::new(
        fireweed_engine::ComposedBackend::new(
            log,
            projection,
            fireweed_engine::InProcessControlPlane::new(),
        )
        .recover()?,
    );
    Ok(Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(blocking_backend::BlockingLibBackend::new(backend)?),
        clock,
    )))
}

/// Open a **sole-owner**, relational SQLite Fireweed handle at `path`. Unlike [`open_sqlite`], this constructor keeps
/// its authoritative projection in relational tables and supports [`Fireweed::discover_active_scopes`],
/// including per-group discovery. Queue creation is atomic across independently opened handles and returns
/// the definition decoded from the durable `queues` catalog. Requires the `sqlite` feature (default).
///
/// Note: this is the **unified** sqlite relational backend (same store on both axes), not the orthogonal
/// [`open_sqlite_sqlite_projection`] matrix cell.
#[cfg(feature = "sqlite")]
pub fn open_sqlite_relational(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    let backend = Arc::new(fireweed_sqlite::composed_sqlite_relational(path)?);
    Ok(Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(blocking_backend::BlockingLibBackend::new(backend)?),
        clock,
    )))
}

/// Open a **sole-owner**, object-log Fireweed handle rooted at `root`, using the shared composed engine
/// with an in-memory projection rebuilt from the authoritative log. Requires the `objectlog` feature
/// (default).
#[cfg(feature = "objectlog")]
pub fn open_objectlog(
    root: impl Into<std::path::PathBuf>,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    let backend = Arc::new(fireweed_objectlog::composed_objectlog_backend(root)?);
    Ok(Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(blocking_backend::BlockingLibBackend::new(backend)?),
        clock,
    )))
}

/// Open an authoritative object log with a disposable PostgreSQL projection behind the public Fireweed
/// facade. The projection is verified against the log before serving, group-commit is flushed by an owned
/// background thread, and dropping the last lifecycle handle shuts that thread down.
///
/// This constructor requires both the `objectlog` and `postgres` features. The SQLite projection variant
/// uses its dedicated `open_composed_sqlite` constructor.
#[cfg(all(feature = "objectlog", feature = "postgres"))]
#[doc(hidden)]
pub(crate) fn open_composed_postgres(
    config: ComposedStorageConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(EngineError::Invalid(
            "open_objectlog_postgres cannot run inside a Tokio runtime; use open_objectlog_postgres_async",
        ));
    }
    open_objectlog_postgres_blocking(config, clock).map(ComposedRuntime::into_fireweed)
}

/// Async-safe variant of [`open_composed_postgres`] for callers already running on Tokio.
///
/// PostgreSQL connection setup and teardown are kept on ordinary OS threads because the synchronous
/// PostgreSQL client owns a private runtime.
#[cfg(all(feature = "objectlog", feature = "postgres"))]
#[doc(hidden)]
pub(crate) async fn open_composed_postgres_async(
    config: ComposedStorageConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    tokio::task::spawn_blocking(move || open_objectlog_postgres_blocking(config, clock))
        .await
        .map_err(|error| {
            EngineError::Storage(format!("object-log PostgreSQL open task failed: {error}"))
        })?
        .map(ComposedRuntime::into_fireweed)
}

/// Open an authoritative object log with a disposable PostgreSQL projection.
#[cfg(all(feature = "objectlog", feature = "postgres"))]
pub fn open_objectlog_postgres(
    config: ObjectLogRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    open_composed_postgres(config.into_storage_config(), clock)
}

/// Async-safe variant of [`open_objectlog_postgres`].
#[cfg(all(feature = "objectlog", feature = "postgres"))]
pub async fn open_objectlog_postgres_async(
    config: ObjectLogRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    open_composed_postgres_async(config.into_storage_config(), clock).await
}

#[cfg(all(feature = "objectlog", feature = "postgres"))]
fn open_objectlog_postgres_blocking(
    config: ComposedStorageConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<ComposedRuntime<blocking_backend::BlockingLibBackend<ObjectLogPostgresBackend>>> {
    use fireweed_engine::{ComposedBackend, InProcessControlPlane};

    config.validate()?;
    if config.response_barrier != CommitResponseBarrier::Strict {
        return Err(EngineError::Unavailable);
    }
    let projection = match &config.projection {
        ComposedProjectionConfig::Postgres { url } => {
            fireweed_postgres::PostgresRelational::connect_in_schema(
                &url.0,
                &derived_postgres_schema_name(&config.namespace),
            )?
        }
        ComposedProjectionConfig::Sqlite { .. } => return Err(EngineError::Unavailable),
    };
    let log = open_composed_object_log(&config, true)?;

    if let Err(error) = validate_objectlog_postgres_catalog(&log, &projection) {
        match config.recovery.incompatible_projection {
            ProjectionRecoveryAction::FailClosed => return Err(error),
            ProjectionRecoveryAction::RebuildProjection => projection.delete_projection()?,
        }
    }
    let backend = ComposedBackend::new(log, projection, InProcessControlPlane::new())
        .with_recovery_max_tail(config.recovery.max_tail_commands)
        .with_group_commit(true)
        .recover()?;
    let flush_interval = backend.group_commit_flush_interval_ms();
    let backend = Arc::new(backend);
    let blocking_backend = blocking_backend::BlockingLibBackend::new(Arc::clone(&backend))?;
    let lifecycle_executor = blocking_backend.executor();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let weak_backend = Arc::downgrade(&backend);
    let thread_stop = Arc::clone(&stop);
    let flusher = std::thread::Builder::new()
        .name(format!("fireweed-composed-{}", config.namespace))
        .spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(flush_interval));
                let Some(backend) = weak_backend.upgrade() else {
                    break;
                };
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64;
                let _ = backend.flush_tick(now_ms);
            }
        })
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let lifecycle = ProjectionLifecycleHandle {
        inner: Arc::new(ProjectionLifecycleHandleInner {
            _config: config.clone(),
            lifecycle: Box::new(ObjectLogPostgresLifecycle {
                backend: Some(Arc::clone(&backend)),
                executor: lifecycle_executor,
                max_tail_commands: config.recovery.max_tail_commands,
                stop,
                flusher: Mutex::new(Some(flusher)),
            }),
        }),
    };
    Ok(ComposedRuntime {
        runtime: RuntimeCore::new(Arc::new(blocking_backend), clock),
        lifecycle,
    })
}

/// Open an authoritative object log with a disposable SQLite projection behind the public Fireweed
/// facade. Both local filesystem and S3-compatible object stores use the same production segmented-log
/// path. [`CommitResponseBarrier::Strict`] makes SQLite durable before success is visible;
/// [`CommitResponseBarrier::AsyncProjection`] acknowledges after the manifest and hot projection, with
/// the owned background flusher checkpointing SQLite.
///
/// The SQLite file is a disposable cache: the returned lifecycle handle can verify it, delete it in place,
/// and rebuild it exactly from authoritative object-log history without changing the live hot projection.
#[cfg(all(feature = "objectlog", feature = "sqlite"))]
#[doc(hidden)]
pub(crate) fn open_composed_sqlite(
    config: ComposedStorageConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    use fireweed_engine::{ComposedBackend, InProcessControlPlane};

    config.validate()?;
    let projection_path = match &config.projection {
        ComposedProjectionConfig::Sqlite { path } => path,
        ComposedProjectionConfig::Postgres { .. } => return Err(EngineError::Unavailable),
    };
    let projection_path = projection_path.to_str().ok_or(EngineError::Invalid(
        "SQLite projection path must be valid UTF-8",
    ))?;
    if let Some(parent) = std::path::Path::new(projection_path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|error| EngineError::Storage(error.to_string()))?;
    }
    let mut projection = fireweed_sqlite::HybridProjectionStore::open(projection_path)?
        .with_strict_apply(config.response_barrier == CommitResponseBarrier::Strict);
    if config.response_barrier == CommitResponseBarrier::AsyncProjection {
        projection =
            projection.with_async_monitor(fireweed_sqlite::HybridAsyncThresholds::default());
    }
    let log = open_composed_object_log(
        &config,
        config.response_barrier == CommitResponseBarrier::Strict,
    )?;

    if let Err(error) = validate_objectlog_sqlite_catalog(&log, projection.sqlite()) {
        match config.recovery.incompatible_projection {
            ProjectionRecoveryAction::FailClosed => return Err(error),
            ProjectionRecoveryAction::RebuildProjection => {
                projection.sqlite().reset_projection()?
            }
        }
    }
    let backend = ComposedBackend::new(log, projection, InProcessControlPlane::new())
        .with_recovery_max_tail(config.recovery.max_tail_commands)
        .with_group_commit(true)
        .recover()?;
    let flush_interval = backend.group_commit_flush_interval_ms();
    let backend = Arc::new(backend);
    let blocking_backend = blocking_backend::BlockingLibBackend::new(Arc::clone(&backend))?;
    let lifecycle_executor = blocking_backend.executor();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let weak_backend = Arc::downgrade(&backend);
    let thread_stop = Arc::clone(&stop);
    let flusher = std::thread::Builder::new()
        .name(format!("fireweed-composed-{}", config.namespace))
        .spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(flush_interval));
                let Some(backend) = weak_backend.upgrade() else {
                    break;
                };
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64;
                let _ = backend.flush_tick(now_ms);
                let _ = backend.try_flush_deferred_projection();
            }
        })
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let lifecycle = ProjectionLifecycleHandle {
        inner: Arc::new(ProjectionLifecycleHandleInner {
            _config: config.clone(),
            lifecycle: Box::new(ObjectLogSqliteLifecycle {
                backend: Arc::clone(&backend),
                executor: lifecycle_executor,
                max_tail_commands: config.recovery.max_tail_commands,
                stop,
                flusher: Mutex::new(Some(flusher)),
            }),
        }),
    };
    Ok(ComposedRuntime {
        runtime: RuntimeCore::new(Arc::new(blocking_backend), clock),
        lifecycle,
    }
    .into_fireweed())
}

/// Open an authoritative object log with a disposable SQLite projection.
#[cfg(all(feature = "objectlog", feature = "sqlite"))]
pub fn open_objectlog_sqlite(
    config: ObjectLogRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    open_composed_sqlite(config.into_storage_config(), clock)
}

/// Open a **sole-owner** PostgreSQL-backed Fireweed handle (log-replay class) at `url`. Requires the `postgres`
/// feature (opt-in). For a durable **multi-instance** deployment use [`open_postgres_coordinated`].
#[cfg(feature = "postgres")]
pub fn open_postgres(url: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    let backend = Arc::new(fireweed_postgres::composed_postgres_backend(url)?);
    Ok(Fireweed::from_runtime(RuntimeCore::new(
        Arc::new(blocking_backend::BlockingLibBackend::new(backend)?),
        clock,
    )))
}

/// Async-safe variant of [`open_postgres`] for callers already running on Tokio.
#[cfg(feature = "postgres")]
pub async fn open_postgres_async(url: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || open_postgres(&url, clock))
        .await
        .map_err(|error| EngineError::Storage(format!("PostgreSQL open task failed: {error}")))?
}

/// Open a PostgreSQL runtime with construction-time storage, schema, identity, and coordination choices.
#[cfg(feature = "postgres")]
pub fn open_postgres_runtime(
    config: PostgresRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    let PostgresRuntimeConfig {
        url,
        schema,
        mode,
        node_id,
        coordination,
    } = config;
    let url = &url.0.0;
    match mode {
        PostgresMode::LogReplay => {
            let backend = match schema.as_deref() {
                Some(schema) => {
                    fireweed_postgres::composed_postgres_backend_in_schema(url, schema)?
                }
                None => fireweed_postgres::composed_postgres_backend(url)?,
            };
            let backend = Arc::new(match node_id {
                Some(node_id) => backend.with_node_id(node_id),
                None => backend,
            });
            let backend = Arc::new(blocking_backend::BlockingLibBackend::new(backend)?);
            match coordination {
                Some(coordination) => {
                    let control_plane: Arc<dyn QueueControlPlane> =
                        Arc::new(match schema.as_deref() {
                            Some(schema) => {
                                fireweed_postgres::PostgresControlPlane::connect_in_schema(
                                    url,
                                    schema,
                                    coordination.control_plane,
                                )?
                            }
                            None => fireweed_postgres::PostgresControlPlane::connect(
                                url,
                                coordination.control_plane,
                            )?,
                        });
                    let executor = backend.executor();
                    Ok(Fireweed::from_runtime(
                        RuntimeCore::with_owned_control_plane_executor(
                            backend,
                            clock,
                            coordination.instance_id,
                            control_plane,
                            executor,
                        ),
                    ))
                }
                None => Ok(Fireweed::from_runtime(RuntimeCore::new(backend, clock))),
            }
        }
        PostgresMode::Relational => {
            let backend = match schema.as_deref() {
                Some(schema) => {
                    fireweed_postgres::PostgresRelationalBackend::connect_in_schema(url, schema)?
                }
                None => fireweed_postgres::PostgresRelationalBackend::connect(url)?,
            };
            let backend = Arc::new(match node_id {
                Some(node_id) => backend.with_node_id(node_id),
                None => backend,
            });
            let backend = Arc::new(blocking_backend::BlockingLibBackend::new(backend)?);
            let queue = match coordination {
                Some(coordination) => {
                    let control_plane: Arc<dyn QueueControlPlane> =
                        Arc::new(match schema.as_deref() {
                            Some(schema) => {
                                fireweed_postgres::PostgresControlPlane::connect_in_schema(
                                    url,
                                    schema,
                                    coordination.control_plane,
                                )?
                            }
                            None => fireweed_postgres::PostgresControlPlane::connect(
                                url,
                                coordination.control_plane,
                            )?,
                        });
                    let executor = backend.executor();
                    RuntimeCore::with_owned_control_plane_executor(
                        backend,
                        clock,
                        coordination.instance_id,
                        control_plane,
                        executor,
                    )
                }
                None => RuntimeCore::new(backend, clock),
            };
            Ok(Fireweed::from_runtime(queue))
        }
    }
}

/// Async-safe variant of [`open_postgres_runtime`] for callers already running on Tokio.
#[cfg(feature = "postgres")]
pub async fn open_postgres_runtime_async(
    config: PostgresRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed> {
    tokio::task::spawn_blocking(move || open_postgres_runtime(config, clock))
        .await
        .map_err(|error| {
            EngineError::Storage(format!("PostgreSQL runtime open task failed: {error}"))
        })?
}

/// Open a **durable multi-instance** coordinated Fireweed handle: builds PostgreSQL storage and the
/// transactional postgres control plane (which binds the storage fence epoch, BQ-23) against `url`, and
/// returns a coordinated [`Fireweed`] for this `instance_id`. Requires the `postgres` feature. The client
/// never names a backend or control plane. (Run each process with a distinct `instance_id`.)
#[cfg(feature = "postgres")]
pub fn open_postgres_coordinated(
    url: &str,
    clock: Arc<dyn Clock>,
    instance_id: OwnerId,
    control_plane_config: fireweed_engine::ControlPlaneConfig,
) -> EngineResult<Fireweed> {
    let backend = Arc::new(fireweed_postgres::composed_postgres_backend(url)?);
    let control_plane: Arc<dyn QueueControlPlane> = Arc::new(
        fireweed_postgres::PostgresControlPlane::connect(url, control_plane_config)?,
    );
    let backend = Arc::new(blocking_backend::BlockingLibBackend::new(backend)?);
    let control_plane_executor = backend.executor();
    Ok(Fireweed::from_runtime(
        RuntimeCore::with_owned_control_plane_executor(
            backend,
            clock,
            instance_id,
            control_plane,
            control_plane_executor,
        ),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    use fireweed_core::{
        ClaimByQueryRequest, FilterOp, IndexDeclaration, IndexDef, IndexType, OrderField,
        OrderingMode, OwnerId, PriorityValue, QueryFilter, QueueDefinition, QueueId, QueueIndex,
        SortDirection, TenantId, TypedValue, UtcTimestamp, WorkerId,
    };

    use super::{ClaimByQueryAt, NewItem, RuntimeCore, SystemClock, apply_owned_renewal_outcomes};
    use crate::EngineResult;
    use fireweed_engine::{
        Clock, EngineError, InMemoryControlPlane, LeaseRenewalOutcome, LeaseState, OwnedSession,
        QueueControlPlane, QueueKey, QueueLease,
    };

    #[cfg(feature = "memory")]
    #[tokio::test(flavor = "current_thread")]
    async fn owned_control_plane_boundary_builds_a_working_coordinated_owner() -> EngineResult<()> {
        let raw = Arc::new(fireweed_memory::composed_memory_backend());
        let bounded = Arc::new(crate::blocking_backend::BlockingLibBackend::new(raw)?);
        let executor = bounded.executor();
        let control_plane = Arc::new(InMemoryControlPlane::default());
        let fireweed = RuntimeCore::with_owned_control_plane_executor(
            bounded,
            Arc::new(SystemClock),
            OwnerId::new("coordinated-owner").unwrap(),
            control_plane,
            executor,
        );
        let definition = query_definition();
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());

        fireweed.create_queue(definition).await?;
        fireweed.push(&queue, NewItem::default()).await?;

        assert!(matches!(
            fireweed.ownership(&queue).await?,
            super::Ownership::Mine { epoch: Some(epoch) } if epoch >= 1
        ));
        assert_eq!(fireweed.metrics(&queue).await?.pending, 1);
        Ok(())
    }

    #[cfg(feature = "memory")]
    #[test]
    fn blocking_lib_backend_concurrent_creates_are_create_or_read() -> EngineResult<()> {
        let raw = Arc::new(fireweed_memory::composed_memory_backend());
        let bounded = Arc::new(crate::blocking_backend::BlockingLibBackend::new(raw)?);
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let backend = Arc::clone(&bounded);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let fireweed = RuntimeCore::new(backend, Arc::new(SystemClock));
                barrier.wait();
                futures::executor::block_on(fireweed.create_queue(query_definition()))
            }));
        }

        let mut created = 0;
        for handle in handles {
            let outcome = handle.join().unwrap()?;
            if outcome.created {
                created += 1;
            }
            assert_eq!(outcome.definition, query_definition());
        }
        assert_eq!(created, 1);
        Ok(())
    }

    #[cfg(feature = "memory")]
    #[test]
    fn blocking_lib_backend_concurrent_incompatible_losers_conflict() -> EngineResult<()> {
        let raw = Arc::new(fireweed_memory::composed_memory_backend());
        let bounded = Arc::new(crate::blocking_backend::BlockingLibBackend::new(raw)?);
        futures::executor::block_on(
            RuntimeCore::new(Arc::clone(&bounded), Arc::new(SystemClock))
                .create_queue(query_definition()),
        )?;

        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let backend = Arc::clone(&bounded);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let fireweed = RuntimeCore::new(backend, Arc::new(SystemClock));
                let mut definition = query_definition();
                definition.ordering_mode = OrderingMode::BoundedRelaxed;
                barrier.wait();
                futures::executor::block_on(fireweed.create_queue(definition))
            }));
        }

        let mut conflicts = 0;
        for handle in handles {
            match handle.join().unwrap() {
                Err(EngineError::QueueDefinitionConflict) => conflicts += 1,
                other => panic!("unexpected create result: {other:?}"),
            }
        }
        assert_eq!(conflicts, 8);
        Ok(())
    }

    #[cfg(feature = "memory")]
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_final_coordinated_handle_never_joins_blocked_control_plane_worker()
    -> EngineResult<()> {
        let raw = Arc::new(fireweed_memory::composed_memory_backend());
        let definition = query_definition();
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        RuntimeCore::new(Arc::clone(&raw), Arc::new(SystemClock))
            .create_queue(definition)
            .await?;

        let bounded = Arc::new(crate::blocking_backend::BlockingLibBackend::new(raw)?);
        let executor = bounded.executor();
        let blocker_executor = executor.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut blocker = Box::pin(blocker_executor.run_for_control_plane_queue(
            &queue,
            move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            },
        ));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(blocker.as_mut().poll(&mut context), Poll::Pending));
        started_rx.recv().unwrap();

        let control_plane = Arc::new(InMemoryControlPlane::default());
        let owner = OwnerId::new("cancelled-waiter-owner").unwrap();
        let fireweed = RuntimeCore::with_owned_control_plane_executor(
            bounded,
            Arc::new(SystemClock),
            owner.clone(),
            control_plane.clone(),
            executor,
        );
        let mut session = Box::pin(fireweed.session_epoch(&queue));
        assert!(matches!(session.as_mut().poll(&mut context), Poll::Pending));
        drop(session);

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            release_tx.send(()).unwrap();
        });
        let drop_started = Instant::now();
        drop(fireweed);
        assert!(
            drop_started.elapsed() < Duration::from_millis(100),
            "final coordinated-handle drop joined a blocked durable-I/O worker"
        );

        blocker.await?;
        releaser.join().unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if control_plane.lease(&queue)?.active_owner_id.as_ref() == Some(&owner) {
                    return Ok::<_, crate::EngineError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted ownership work did not complete after waiter/handle drop")?;
        Ok(())
    }

    fn ts(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(seconds, 0).unwrap()
    }

    struct PanicClock(AtomicBool);

    impl PanicClock {
        fn new() -> Self {
            Self(AtomicBool::new(false))
        }
    }

    impl Clock for PanicClock {
        fn now(&self) -> UtcTimestamp {
            self.0.store(true, Ordering::SeqCst);
            panic!(
                "claim_by_query_at must not consult the handle clock when explicit times are set"
            );
        }
    }

    fn query_definition() -> QueueDefinition {
        let mut definition = QueueDefinition {
            tenant_id: TenantId::new("t1").unwrap(),
            queue_id: QueueId::new("q1").unwrap(),
            priority_model: fireweed_core::PriorityModel {
                kind: fireweed_core::PriorityModelKind::Int64,
                direction: fireweed_core::PriorityDirection::Ascending,
                tie_breaker: fireweed_core::PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: fireweed_core::OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: fireweed_core::EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: fireweed_core::RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: fireweed_core::RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: true,
        };
        definition.typed_indexes = vec![QueueIndex {
            name: "by_rank".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "rank".to_string(),
                index_type: IndexType::Integer,
                unique: false,
            }),
        }];
        definition
    }

    #[cfg(all(feature = "objectlog", feature = "postgres"))]
    #[test]
    fn objectlog_postgres_schema_name_is_legal_bounded_and_deterministic() {
        let namespaces = vec![
            "short".to_string(),
            "punctuation-heavy:-/namespace.with spaces".to_string(),
            "ümlaut/雪/namespace:with:unicode".to_string(),
            "a".repeat(256),
        ];
        let mut seen = HashSet::new();

        for namespace in namespaces {
            let schema = super::derived_postgres_schema_name(&namespace);
            assert!(schema.len() <= 63, "{schema}");
            assert!(
                schema
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            );
            assert!(schema.starts_with("fireweed_"));
            assert!(
                seen.insert(schema.clone()),
                "schema derivation collided for {namespace:?}"
            );
            assert_eq!(schema, super::derived_postgres_schema_name(&namespace));
        }
    }

    #[cfg(feature = "objectlog")]
    #[test]
    fn object_log_namespace_encoding_stays_hex_bytes() {
        assert_eq!(super::object_log_namespace("a-b:/é"), "612d623a2fc3a9");
    }

    fn query_request(request_id: &str) -> ClaimByQueryRequest {
        ClaimByQueryRequest {
            index: Some("by_rank".to_string()),
            filters: vec![QueryFilter {
                field: "rank".to_string(),
                op: FilterOp::Gte,
                value: TypedValue::Integer(0),
            }],
            order_by: OrderField {
                field: "rank".to_string(),
                direction: SortDirection::Ascending,
            },
            max_items: 10,
            lease_duration_ms: 30_000,
            worker_id: WorkerId::new("query-worker").unwrap(),
            request_id: Some(fireweed_core::RequestId::new(request_id).unwrap()),
        }
    }

    #[test]
    fn mixed_renewal_outcomes_process_all_rows_and_preserve_transient_sessions() {
        let owner = fireweed_core::OwnerId::new("owner").unwrap();
        let queues: Vec<QueueKey> = ["assigned", "draining", "error", "fenced", "missing"]
            .into_iter()
            .map(|name| QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new(name).unwrap()))
            .collect();
        let sessions = std::sync::Mutex::new(
            queues
                .iter()
                .map(|queue| {
                    (
                        queue.clone(),
                        OwnedSession {
                            owner: owner.clone(),
                            queue: queue.clone(),
                            lease_epoch: 1,
                            fence_epoch: 1,
                        },
                    )
                })
                .collect::<HashMap<_, _>>(),
        );
        let draining = std::sync::Mutex::new(queues.iter().cloned().collect::<HashSet<_>>());
        let lease = |state| QueueLease {
            state,
            active_owner_id: Some(owner.clone()),
            target_owner_id: None,
            assignment_epoch: 1,
            lease_expires_at: Some(ts(15)),
        };
        let error = crate::EngineError::Storage("transient row".into());
        let result = apply_owned_renewal_outcomes(
            &sessions,
            &draining,
            queues.iter().cloned().map(|queue| (queue, 1)).collect(),
            vec![
                LeaseRenewalOutcome::Renewed(lease(LeaseState::Assigned)),
                LeaseRenewalOutcome::Renewed(lease(LeaseState::Draining)),
                LeaseRenewalOutcome::Error(error.clone()),
                LeaseRenewalOutcome::Fenced,
                LeaseRenewalOutcome::Missing,
            ],
        );
        assert_eq!(result, Err(error));
        let sessions = sessions.lock().unwrap();
        assert!(sessions.contains_key(&queues[0]));
        assert!(sessions.contains_key(&queues[1]));
        assert!(
            sessions.contains_key(&queues[2]),
            "transient error retains session"
        );
        assert!(!sessions.contains_key(&queues[3]));
        assert!(!sessions.contains_key(&queues[4]));
        let draining = draining.lock().unwrap();
        assert!(
            !draining.contains(&queues[0]),
            "assigned clears drain state"
        );
        assert!(
            draining.contains(&queues[1]),
            "draining outcome is observed"
        );
        assert!(
            draining.contains(&queues[2]),
            "error leaves prior drain state unchanged"
        );
        assert!(!draining.contains(&queues[3]));
        assert!(!draining.contains(&queues[4]));
    }

    #[tokio::test]
    async fn claim_by_query_at_uses_explicit_times_and_bypasses_clock() -> EngineResult<()> {
        let backend = Arc::new(fireweed_memory::composed_memory_backend());
        let setup = RuntimeCore::new(Arc::clone(&backend), Arc::new(SystemClock));
        setup.create_queue(query_definition()).await?;
        let shard = fireweed_engine::QueueKey::new(
            TenantId::new("t1").unwrap(),
            QueueId::new("q1").unwrap(),
        );

        let due = NewItem {
            priority: Some(PriorityValue::Int64(1)),
            entity: Some(serde_json::json!({"rank": 1})),
            ..Default::default()
        };
        let later = NewItem {
            priority: Some(PriorityValue::Int64(2)),
            not_before: Some(ts(200)),
            entity: Some(serde_json::json!({"rank": 2})),
            ..Default::default()
        };
        let pushed = setup.push_batch(&shard, vec![due, later]).await?;
        let fireweed = RuntimeCore::new(backend, Arc::new(PanicClock::new()));

        let claimed = fireweed
            .claim_by_query_at(
                &shard,
                query_request("explicit-times"),
                ClaimByQueryAt::new()
                    .eligibility_time(ts(150))
                    .lease_time(ts(1_000)),
            )
            .await?;

        assert_eq!(claimed.items.len(), 1);
        assert_eq!(claimed.items[0].lease_expires_at, ts(1_030));
        assert_eq!(claimed.items[0].item_version, 2);
        assert_eq!(claimed.items[0].item_id, pushed[0]);
        Ok(())
    }

    #[tokio::test]
    async fn facade_enforces_persisted_push_and_claim_batch_limits() -> EngineResult<()> {
        let backend = Arc::new(fireweed_memory::composed_memory_backend());
        let fireweed = RuntimeCore::new(backend, Arc::new(SystemClock));
        let mut definition = query_definition();
        definition.max_push_batch_size = 2;
        definition.max_claim_batch_size = 2;
        fireweed.create_queue(definition).await?;
        let shard = fireweed_engine::QueueKey::new(
            TenantId::new("t1").unwrap(),
            QueueId::new("q1").unwrap(),
        );

        let too_many = vec![NewItem::default(), NewItem::default(), NewItem::default()];
        assert_eq!(
            fireweed.push_batch(&shard, too_many).await.unwrap_err(),
            crate::EngineError::BatchTooLarge
        );
        fireweed
            .push_batch(&shard, vec![NewItem::default(), NewItem::default()])
            .await?;
        assert_eq!(
            fireweed.claim(&shard, 3, 1_000).await.unwrap_err(),
            crate::EngineError::BatchTooLarge
        );
        assert_eq!(fireweed.claim(&shard, 2, 1_000).await?.len(), 2);
        Ok(())
    }
}
