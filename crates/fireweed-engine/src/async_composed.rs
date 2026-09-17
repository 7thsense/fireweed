//! Narrow async mutation-path scaffolding from ADR-017.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fireweed_core::{
    BodyHash, CohortId, GateKeyPolicy, ItemId, ItemState, LeaseToken, PriorityModelKind,
    PriorityValue, QueueDefinition, RequestId, TypedValue, UtcTimestamp,
};

use crate::{
    AppendAdmissionClass, AsyncCohortFinalizeRequest, AsyncCohortLifecyclePlanner,
    AsyncCohortRenewRequest, AsyncCommitStrategy, AsyncReclaimPlanner, AsyncReclaimRequest,
    BatchUpdateRequest, BatchUpdateResponse, BatchUpdateSnapshotItem, ClaimCommand, ClaimRequest,
    ClaimUnit, Claimed, ClaimedItem, CohortClaimCommand, CommandChecksum, CommandEnvelope,
    CommandId, CoordinationError, DispatchError, DurabilityClass, EngineError, EngineResult,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, KeyedQueueGate,
    MUTATION_SEQUENCER_WAIT_RESOURCE, MutationGenerationBatch, MutationGenerationKind,
    MutationSequencer, MutationTicket, NoAsyncCohortLifecyclePlanner, OwnedTask,
    OwnedTaskDispatcher, PreparedAsyncCommitStrategy, PushCommand, PushItem, PushSpec,
    QueueCommand, QueueGateError, QueueKey, RawCommitFault, RawCommitOutcome, RawCommitRequest,
    RequestOutcome, TaskOutcomeError, UpdateFieldsBatchCommand, compile_entity_schema,
    plan_batch_update, validate_claim_compatibility, validate_entity, validate_gate_push,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncCommitSubmitError {
    Prepare(EngineError),
    Admission(QueueGateError),
    Dispatch(DispatchError),
    Outcome(TaskOutcomeError),
}

/// Failure of a typed async claim operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncClaimError {
    Submit(AsyncCommitSubmitError),
    /// Definition lookup, compatibility validation, or planning failed before strategy invocation.
    BeforeCommit(EngineError),
    /// The strategy reported failure while owning the commit. This does not itself prove rollback: a
    /// substrate error at the commit boundary may be an unknown outcome and must be resolved by replay.
    Commit(EngineError),
    /// The strategy returned durable success, but its footprint or the response barrier failed validation.
    AfterCommit {
        stage: AsyncClaimPostCommitStage,
        source: EngineError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncClaimPostCommitStage {
    CommitOutcome,
    Render,
    RenderValidation,
}

/// The owned result of claim selection and command construction.
///
/// A planner may select nothing or produce one typed claim commit. The composed backend validates that
/// the commit is a claim for the requested queue, candidates, lease, and fence before routing it through
/// its injected [`AsyncCommitStrategy`].
pub struct AsyncClaimPlan {
    kind: AsyncClaimPlanKind,
}

enum AsyncClaimPlanKind {
    Empty,
    Commit {
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
        cohort_id: Option<CohortId>,
    },
}

impl AsyncClaimPlan {
    pub fn empty() -> Self {
        Self {
            kind: AsyncClaimPlanKind::Empty,
        }
    }

    pub fn commit(
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
        cohort_id: Option<CohortId>,
    ) -> Self {
        Self {
            kind: AsyncClaimPlanKind::Commit {
                request,
                item_ids,
                cohort_id,
            },
        }
    }
}

/// Backend-owned claim planning and rendering capability.
///
/// Implementations perform selection and construct a claim envelope, but never commit it. This split keeps
/// the durable mutation authority in [`AsyncCommitStrategy`] while allowing native-async projections to
/// perform selection and response rendering without a blocking bridge.
pub trait AsyncClaimPlanner: Send + Sync + 'static {
    /// Load the authoritative definition used by engine-owned compatibility validation.
    fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>>;

    fn plan_claim(
        &self,
        request: ClaimRequest,
        unit: ClaimUnit,
    ) -> OwnedTask<EngineResult<AsyncClaimPlan>>;

    fn render_claimed(
        &self,
        shard: QueueKey,
        item_ids: Vec<ItemId>,
    ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>>;
}

/// Marker used by a raw-commit-only composed backend.
pub struct NoAsyncClaimPlanner;

/// One owned invocation of the async push path.
#[derive(Debug, Clone)]
pub struct AsyncPushRequest {
    pub shard: QueueKey,
    pub request_id: Option<RequestId>,
    pub items: Vec<PushSpec>,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushFingerprint {
    pub canonical_sha256: [u8; 32],
    pub legacy_body_hash: BodyHash,
}

/// Planner result for a push. Constructors keep the representation typed and engine-validated.
pub struct AsyncPushPlan {
    kind: AsyncPushPlanKind,
}

enum AsyncPushPlanKind {
    Replay(Vec<ItemId>),
    Commit {
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
    },
}

impl AsyncPushPlan {
    pub fn replay(item_ids: Vec<ItemId>) -> Self {
        Self {
            kind: AsyncPushPlanKind::Replay(item_ids),
        }
    }

    pub fn commit(request: RawCommitRequest, item_ids: Vec<ItemId>) -> Self {
        Self {
            kind: AsyncPushPlanKind::Commit { request, item_ids },
        }
    }
}

/// Push plan after queue-local admission, before the durable append.
///
/// Object-log products commit this without holding the admit permit so concurrent
/// same-shard appends can share one packed PUT. Sqlite-log keeps plan+commit
/// under the permit via [`AsyncComposedBackend::push`].
#[derive(Debug, Clone)]
pub enum PreparedPush {
    Replay(Vec<ItemId>),
    Commit {
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
    },
}

/// Claim plan after queue-local admission, before the durable append.
pub enum PreparedClaim {
    Empty,
    Commit {
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
        cohort_id: Option<CohortId>,
    },
}

impl PreparedClaim {
    /// Stamp `authority_first` on new item Claim envelopes. Legacy outbox recoveries stay false.
    pub fn with_item_authority_first(self) -> Self {
        match self {
            Self::Empty => Self::Empty,
            Self::Commit {
                request,
                item_ids,
                cohort_id,
            } => {
                let (shard, mut commands, epoch, fault, admission) =
                    request.into_parts_with_append_admission();
                for envelope in &mut commands {
                    if let QueueCommand::Claim(command) = &mut envelope.command {
                        command.authority_first = true;
                    }
                }
                Self::Commit {
                    request: RawCommitRequest::new(shard, commands, epoch)
                        .with_fault(fault)
                        .with_append_admission(admission),
                    item_ids,
                    cohort_id,
                }
            }
        }
    }
}

/// Grouped/cohort/item full-row result retained through append.
///
/// S5 item/group/cohort continuation returns this carrier after apply and must not borrow a
/// projection handle or committed pool.
#[derive(Debug, Clone)]
pub enum PreparedClaimedResult {
    Empty,
    Retained(Claimed),
}

impl PreparedClaimedResult {
    pub fn empty() -> Self {
        Self::Empty
    }

    /// Materialize a retained grouped/cohort response from already-rendered full rows.
    ///
    /// Validates count/order/token/expiry, then applies cohort shaping. The caller supplies the
    /// snapshot rows; this constructor takes no projection handle.
    pub fn from_rendered(
        request: &ClaimRequest,
        item_ids: &[ItemId],
        items: Vec<ClaimedItem>,
        cohort_id: Option<CohortId>,
    ) -> EngineResult<Self> {
        if item_ids.is_empty() {
            return Ok(Self::Empty);
        }
        Ok(Self::Retained(finish_retained_grouped_cohort_claim(
            request, item_ids, items, cohort_id,
        )?))
    }

    /// Post-apply continuation: return the retained envelope with no projection read.
    pub fn into_claimed(self) -> Claimed {
        match self {
            Self::Empty => Claimed::default(),
            Self::Retained(claimed) => claimed,
        }
    }
}

/// Sequencer compatibility: same-kind Push/Update overlay; keyed/complex mutations stay singleton.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MutationSequencerKey {
    Compatible(MutationGenerationKind),
    Singleton(u64),
}

/// S3s-derived queued-generation wait (floor 500 ms, structural cap 255 s). Not activated until S3c.
pub const MUTATION_SEQUENCER_DEFAULT_MAX_WAIT: Duration = crate::S3S_DERIVED_TURN_WAIT;

/// Committed driver-snapshot facts plus in-generation overlay inputs.
///
/// `leased_ids` / `terminal_ids` are the unpublished Update overlay: Claim assigns, Complete/Fail
/// consumes. They replace an exclusive selection fence for ordinary item Claim.
#[derive(Debug, Clone)]
pub struct MutationDriverSnapshot {
    pub definition: QueueDefinition,
    pub paused_drain_intake: bool,
    pub client_keys: HashSet<String>,
    pub request_fingerprints: HashMap<RequestId, BodyHash>,
    pub unique_index_values: HashSet<String>,
    pub group_counts: HashMap<String, u64>,
    pub batch_items: Vec<BatchUpdateSnapshotItem>,
    pub leased_ids: HashSet<ItemId>,
    pub terminal_ids: HashSet<ItemId>,
}

/// One mutation that may join a packed add (`Push`) or update generation.
///
/// Update work is field-update, ordinary item Claim, and Complete/Fail. Those share one overlay
/// because they are the same bulk modify: assign, rewrite, or consume candidate rows.
#[derive(Debug, Clone)]
pub enum MutationGenerationWork {
    Push {
        request: AsyncPushRequest,
        fingerprint: Option<PushFingerprint>,
    },
    BatchUpdate {
        shard: QueueKey,
        request: BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: u64,
        fingerprint: BodyHash,
        command_id: CommandId,
    },
    Claim {
        id: u64,
        request: ClaimRequest,
    },
    Finalize {
        shard: QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: u64,
        command_id: CommandId,
    },
    ItemMutation {
        id: u64,
        shard: QueueKey,
        request: crate::ItemMutationRequest,
        expected_epoch: Option<u64>,
    },
    Singleton {
        id: u64,
        commit: RawCommitRequest,
    },
}

/// Per-request generation outcome. Neighbors stay independent.
#[derive(Debug, Clone)]
pub enum MutationGenerationMemberOutcome {
    Push(PreparedPush),
    /// Overlay accepted an unprepared add. The driver allocates ids/blobs before append.
    PushAccepted,
    BatchUpdate {
        request: RawCommitRequest,
        response: BatchUpdateResponse,
    },
    /// Overlay accepted an unprepared Claim. The driver selects against the serving reader next.
    ClaimAccepted {
        id: u64,
    },
    Claim {
        id: u64,
        request: Option<RawCommitRequest>,
        claimed: Claimed,
    },
    Finalize {
        request: RawCommitRequest,
    },
    Singleton {
        request: RawCommitRequest,
    },
    ItemMutation {
        id: u64,
        result: EngineResult<crate::ItemMutationResponse>,
    },
    Rejected(EngineError),
}

#[derive(Debug, Clone)]
pub struct MutationGenerationMember {
    pub outcome: MutationGenerationMemberOutcome,
}

/// Inert generation after overlay validation: slot/connection released, sequencer retained.
pub struct PreparedMutationGeneration<K, C, R>
where
    K: Clone + Eq + std::hash::Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    pub members: Vec<MutationGenerationMember>,
    pub slot_and_connection_released: bool,
    pub sequencer: MutationGenerationBatch<K, C, R>,
}

impl MutationGenerationWork {
    pub fn sequencer_key(&self) -> MutationSequencerKey {
        match self {
            Self::Push { .. } => MutationSequencerKey::Compatible(MutationGenerationKind::Push),
            Self::BatchUpdate { .. } => {
                MutationSequencerKey::Compatible(MutationGenerationKind::Rewrite)
            }
            Self::Claim { .. } | Self::Finalize { .. } => {
                MutationSequencerKey::Compatible(MutationGenerationKind::Update)
            }
            Self::ItemMutation { .. } => {
                MutationSequencerKey::Compatible(MutationGenerationKind::Addressed)
            }
            Self::Singleton { id, .. } => MutationSequencerKey::Singleton(*id),
        }
    }

    pub fn queue(&self) -> QueueKey {
        match self {
            Self::Push { request, .. } => request.shard.clone(),
            Self::BatchUpdate { shard, .. }
            | Self::Finalize { shard, .. }
            | Self::ItemMutation { shard, .. } => shard.clone(),
            Self::Claim { request, .. } => request.shard.clone(),
            Self::Singleton { commit, .. } => commit.shard().clone(),
        }
    }

    pub fn items(&self) -> usize {
        match self {
            Self::Push { request, .. } => request.items.len(),
            Self::BatchUpdate { request, .. } => request.updates.len(),
            Self::Claim { request, .. } => request.max_items.max(1),
            Self::Finalize { outcomes, .. } => outcomes.len().max(1),
            Self::ItemMutation { request, .. } => match &request.operation {
                crate::ItemMutationOperation::Addressed { entries } => entries.len().max(1),
                crate::ItemMutationOperation::SelectFirst { .. } => 1,
            },
            Self::Singleton { .. } => 1,
        }
    }

    pub fn response_bytes(&self) -> usize {
        match self {
            Self::Push { request, .. } => request.items.iter().map(push_spec_rendered_bytes).sum(),
            Self::BatchUpdate { request, .. } => request.updates.len().saturating_mul(64),
            Self::Claim { request, .. } => request.max_items.saturating_mul(4 * 1024),
            Self::Finalize { outcomes, .. } => outcomes.len().saturating_mul(64),
            Self::ItemMutation { .. } => self.items().saturating_mul(4 * 1024),
            Self::Singleton { .. } => 64,
        }
    }
}

fn push_spec_rendered_bytes(item: &crate::PushSpec) -> usize {
    item.payload.as_ref().map_or(0, |payload| payload.len())
        + item.fields.values().map(|value| value.len()).sum::<usize>()
}

/// Abort a not-yet-planned queued generation with retryable wait and no durable effect.
///
/// One internal attempt: the caller retries the original request at the FIFO tail.
pub fn abort_unplanned_generation_on_deadline<K, C, R>(
    tickets: Vec<MutationTicket<K, C, R>>,
    queued_at: Instant,
    max_wait: Duration,
) -> Result<Vec<MutationTicket<K, C, R>>, EngineError>
where
    K: Clone + Eq + std::hash::Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    if queued_at.elapsed() >= max_wait {
        drop(tickets);
        return Err(EngineError::Backpressure {
            resource: MUTATION_SEQUENCER_WAIT_RESOURCE,
        });
    }
    Ok(tickets)
}

fn coordination_backpressure(error: CoordinationError) -> EngineError {
    EngineError::Backpressure {
        resource: error.resource(),
    }
}

/// Admit FIFO work into the mutation sequencer. Does not take a selection fence.
pub fn admit_inert_mutation_generation(
    sequencer: &MutationSequencer<QueueKey, MutationSequencerKey, MutationGenerationWork>,
    ingress: crate::MutationIngress,
    works: Vec<Arc<MutationGenerationWork>>,
) -> Result<Vec<MutationTicket<QueueKey, MutationSequencerKey, MutationGenerationWork>>, EngineError>
{
    let mut tickets = Vec::with_capacity(works.len());
    for work in works {
        let queue = work.queue();
        let items = work.items();
        let response_bytes = work.response_bytes();
        let key = work.sequencer_key();
        match sequencer.admit(queue, key, ingress, work, items, response_bytes) {
            Ok(ticket) => tickets.push(ticket),
            Err(error) => return Err(coordination_backpressure(error)),
        }
    }
    Ok(tickets)
}

/// Validate up to eight compatible requests in FIFO against one snapshot plus overlay.
///
/// A rejected member does not alter the overlay or neighboring outcomes. Renders separate
/// envelopes. The caller must drop the global slot/connection before co-sealed append.
pub fn validate_inert_mutation_generation(
    snapshot: &MutationDriverSnapshot,
    works: &[MutationGenerationWork],
) -> EngineResult<Vec<MutationGenerationMember>> {
    Ok(validate_inert_mutation_generation_folding(snapshot, works)?.0)
}

/// Same FIFO overlay as [`validate_inert_mutation_generation`], plus the folded identity
/// facts the next generation must see before this generation's apply covers.
pub fn validate_inert_mutation_generation_folding(
    snapshot: &MutationDriverSnapshot,
    works: &[MutationGenerationWork],
) -> EngineResult<(Vec<MutationGenerationMember>, MutationDriverSnapshot)> {
    if works.len() > crate::CLAIM_GENERATION_MAX_REQUESTS {
        return Err(EngineError::Backpressure {
            resource: crate::MUTATION_SEQUENCER_RESOURCE,
        });
    }
    let mut overlay = MutationGenerationOverlay::from_snapshot(snapshot);
    let mut members = Vec::with_capacity(works.len());
    for work in works {
        members.push(MutationGenerationMember {
            outcome: overlay.validate_one(work),
        });
    }
    Ok((
        members,
        MutationDriverSnapshot {
            definition: snapshot.definition.clone(),
            paused_drain_intake: snapshot.paused_drain_intake,
            client_keys: overlay.client_keys,
            request_fingerprints: overlay.request_fingerprints,
            unique_index_values: overlay.unique_index_values,
            group_counts: overlay.group_counts,
            batch_items: overlay.batch_items,
            leased_ids: overlay.leased_ids,
            terminal_ids: overlay.terminal_ids,
        },
    ))
}

/// Record that the shared driver slot and pooled connection are gone; retain only the sequencer.
pub fn retain_sequencer_after_slot_release<K, C, R>(
    members: Vec<MutationGenerationMember>,
    sequencer: MutationGenerationBatch<K, C, R>,
) -> PreparedMutationGeneration<K, C, R>
where
    K: Clone + Eq + std::hash::Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    PreparedMutationGeneration {
        members,
        slot_and_connection_released: true,
        sequencer,
    }
}

struct MutationGenerationOverlay<'a> {
    snapshot: &'a MutationDriverSnapshot,
    client_keys: HashSet<String>,
    request_fingerprints: HashMap<RequestId, BodyHash>,
    unique_index_values: HashSet<String>,
    group_counts: HashMap<String, u64>,
    batch_items: Vec<BatchUpdateSnapshotItem>,
    leased_ids: HashSet<ItemId>,
    terminal_ids: HashSet<ItemId>,
}

impl<'a> MutationGenerationOverlay<'a> {
    fn from_snapshot(snapshot: &'a MutationDriverSnapshot) -> Self {
        Self {
            snapshot,
            client_keys: snapshot.client_keys.clone(),
            request_fingerprints: snapshot.request_fingerprints.clone(),
            unique_index_values: snapshot.unique_index_values.clone(),
            group_counts: snapshot.group_counts.clone(),
            batch_items: snapshot.batch_items.clone(),
            leased_ids: snapshot.leased_ids.clone(),
            terminal_ids: snapshot.terminal_ids.clone(),
        }
    }

    fn validate_one(&mut self, work: &MutationGenerationWork) -> MutationGenerationMemberOutcome {
        match work {
            MutationGenerationWork::Push {
                request,
                fingerprint,
            } => self.validate_push(request, *fingerprint),
            MutationGenerationWork::BatchUpdate {
                shard,
                request,
                now,
                expected_epoch,
                fingerprint,
                command_id,
            } => self.validate_batch_update(
                shard,
                request,
                *now,
                *expected_epoch,
                *fingerprint,
                command_id.clone(),
            ),
            MutationGenerationWork::Claim { id, .. } => {
                MutationGenerationMemberOutcome::ClaimAccepted { id: *id }
            }
            MutationGenerationWork::Finalize {
                shard,
                outcomes,
                now,
                expected_epoch,
                command_id,
            } => self.validate_finalize(shard, outcomes, *now, *expected_epoch, command_id.clone()),
            // This family requires full row snapshots and is planned by the
            // adapter's exclusive driver, never by the identity-only overlay.
            MutationGenerationWork::ItemMutation { id, .. } => {
                MutationGenerationMemberOutcome::ItemMutation {
                    id: *id,
                    result: Err(EngineError::Unavailable),
                }
            }
            MutationGenerationWork::Singleton { commit, .. } => {
                MutationGenerationMemberOutcome::Singleton {
                    request: commit.clone(),
                }
            }
        }
    }

    fn validate_push(
        &mut self,
        request: &AsyncPushRequest,
        fingerprint: Option<PushFingerprint>,
    ) -> MutationGenerationMemberOutcome {
        if self.snapshot.paused_drain_intake {
            return MutationGenerationMemberOutcome::Rejected(EngineError::Paused {
                drain_intake: true,
            });
        }
        if let (Some(request_id), Some(_)) = (request.request_id.as_ref(), fingerprint)
            && self.request_fingerprints.contains_key(request_id)
        {
            return MutationGenerationMemberOutcome::Rejected(EngineError::RequestIdConflict);
        }
        if let Err(error) = validate_push_shape(&self.snapshot.definition, &request.items) {
            return MutationGenerationMemberOutcome::Rejected(error);
        }
        let mut keys = Vec::new();
        let mut indexes = Vec::new();
        let mut groups = Vec::new();
        for item in &request.items {
            let Some(key) = item.client_item_key.as_ref() else {
                continue;
            };
            let key = key.as_str().to_string();
            if self.client_keys.contains(&key) {
                return MutationGenerationMemberOutcome::Rejected(EngineError::Conflict);
            }
            keys.push(key);
            for (name, value) in &item.index_fields {
                let encoded = format!("{name}={value:?}");
                if self.unique_index_values.contains(&encoded) {
                    return MutationGenerationMemberOutcome::Rejected(EngineError::Conflict);
                }
                indexes.push(encoded);
            }
            if let Some(group) = &item.group_key {
                let next = self
                    .group_counts
                    .get(group.as_str())
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1);
                if self
                    .snapshot
                    .definition
                    .max_eligible_group_size
                    .is_some_and(|max| next > max)
                {
                    return MutationGenerationMemberOutcome::Rejected(EngineError::Invalid(
                        "group batch exceeds queue limit",
                    ));
                }
                groups.push((group.as_str().to_string(), next));
            }
        }
        for key in keys {
            self.client_keys.insert(key);
        }
        self.unique_index_values.extend(indexes);
        for (group, next) in groups {
            self.group_counts.insert(group, next);
        }
        if let (Some(request_id), Some(fingerprint)) = (request.request_id.clone(), fingerprint) {
            self.request_fingerprints
                .insert(request_id, fingerprint.legacy_body_hash);
        }
        MutationGenerationMemberOutcome::PushAccepted
    }

    fn validate_batch_update(
        &mut self,
        shard: &QueueKey,
        request: &BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: u64,
        fingerprint: BodyHash,
        command_id: CommandId,
    ) -> MutationGenerationMemberOutcome {
        if let Some(stored) = self.request_fingerprints.get(&request.request_id) {
            if *stored == fingerprint {
                return MutationGenerationMemberOutcome::Rejected(EngineError::NotFound);
            }
            return MutationGenerationMemberOutcome::Rejected(EngineError::RequestIdConflict);
        }
        let plan = plan_batch_update(
            &self.snapshot.definition,
            true,
            request.updates.clone(),
            self.batch_items.clone(),
        );
        let accepted = plan
            .outcomes
            .iter()
            .any(|outcome| matches!(outcome, crate::BatchUpdateOutcome::Updated { .. }));
        if accepted {
            for outcome in &plan.outcomes {
                if let crate::BatchUpdateOutcome::Updated {
                    item_id,
                    item_version,
                    ..
                } = outcome
                    && let Some(item) = self
                        .batch_items
                        .iter_mut()
                        .find(|item| item.item_id == *item_id)
                {
                    item.item_version = *item_version;
                }
            }
            self.request_fingerprints
                .insert(request.request_id.clone(), fingerprint);
        }
        let updates: Vec<_> = plan
            .commands
            .into_iter()
            .map(|(_, update)| update)
            .collect();
        let item_ids: Vec<_> = updates.iter().map(|update| update.item_id).collect();
        let response = BatchUpdateResponse {
            request_id: request.request_id.clone(),
            results: plan.outcomes,
        };
        let response_payload = match serde_json::to_string(&response) {
            Ok(payload) => payload,
            Err(error) => {
                return MutationGenerationMemberOutcome::Rejected(EngineError::Storage(
                    error.to_string(),
                ));
            }
        };
        let envelope = CommandEnvelope {
            command_id,
            request_id: Some(request.request_id.clone()),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: Some(crate::RequestOutcome::BatchUpdate { response_payload }),
            item_ids,
            command: QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        MutationGenerationMemberOutcome::BatchUpdate {
            request: RawCommitRequest::new(shard.clone(), vec![envelope], expected_epoch),
            response,
        }
    }

    fn validate_finalize(
        &mut self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
        now: UtcTimestamp,
        expected_epoch: u64,
        command_id: CommandId,
    ) -> MutationGenerationMemberOutcome {
        if outcomes.is_empty() {
            return MutationGenerationMemberOutcome::Rejected(EngineError::Invalid(
                "finalize item batch must not be empty",
            ));
        }
        let mut sealed = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            if matches!(
                outcome.kind,
                FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm
            ) {
                return MutationGenerationMemberOutcome::Rejected(EngineError::Invalid(
                    "retry/release/rearm stay on the singleton path",
                ));
            }
            if outcome.not_before.is_some() {
                return MutationGenerationMemberOutcome::Rejected(EngineError::Invalid(
                    "not_before is invalid for finalize disposition",
                ));
            }
            let applied_state = match outcome.kind {
                FinalizeKind::Complete => ItemState::Complete,
                FinalizeKind::Fail => ItemState::Failed,
                FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm => {
                    unreachable!("singleton finalize kinds rejected above")
                }
            };
            self.leased_ids.remove(&outcome.item_id);
            self.terminal_ids.insert(outcome.item_id);
            sealed.push(FinalizeOutcome {
                item_id: outcome.item_id,
                kind: outcome.kind,
                applied_state: Some(applied_state),
                not_before: None,
            });
        }
        let item_ids: Vec<_> = sealed.iter().map(|outcome| outcome.item_id).collect();
        let envelope = CommandEnvelope {
            command_id,
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command: QueueCommand::Finalize(FinalizeCommand { outcomes: sealed }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        MutationGenerationMemberOutcome::Finalize {
            request: RawCommitRequest::new(shard.clone(), vec![envelope], expected_epoch),
        }
    }
}

/// Finalize plan after queue-local admission, before the durable append.
pub struct PreparedFinalize {
    pub request: RawCommitRequest,
    pub expected_finalize_outcomes: Option<Vec<crate::FinalizeOutcome>>,
}

/// Construction-injected push preparation capability. It may resolve retained replay or allocate IDs
/// and build an envelope, but it has no durable commit authority.
pub trait AsyncPushPlanner: Send + Sync + 'static {
    fn supports_gates(&self) -> bool;

    fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>>;

    /// Resolve idempotency and, only for fresh work, reserve IDs and construct the push envelope.
    fn plan_push(
        &self,
        request: AsyncPushRequest,
        definition: QueueDefinition,
        fingerprint: Option<PushFingerprint>,
    ) -> OwnedTask<EngineResult<AsyncPushPlan>>;
}

/// Marker used when typed async push was not injected.
pub struct NoAsyncPushPlanner;

/// One ordinary-item lease renewal owned by the async backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewTarget {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeTarget {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
    pub item_version: u64,
    pub kind: crate::FinalizeKind,
    pub not_before: Option<UtcTimestamp>,
}

#[derive(Debug, Clone)]
pub struct AsyncFinalizeRequest {
    pub shard: QueueKey,
    pub targets: Vec<FinalizeTarget>,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
/// Internal typed-composition request for [`crate::PurgePort`] parity.
///
/// This request carries the already-resolved item ids through the async planner/commit boundary and
/// returns only the number removed. It is not the public API-001 `PurgeItems` request contract, whose
/// key-or-id targeting, request-id replay, and per-item `purged`/`not_found` results belong at the API
/// adapter boundary.
pub struct AsyncPurgeRequest {
    pub shard: QueueKey,
    pub item_ids: Vec<ItemId>,
    pub force: bool,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AsyncRenewRequest {
    pub shard: QueueKey,
    pub targets: Vec<RenewTarget>,
    pub new_lease_expires_at: UtcTimestamp,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AsyncReassignRequest {
    pub shard: QueueKey,
    pub targets: Vec<RenewTarget>,
    pub new_lease_token: LeaseToken,
    pub new_lease_expires_at: UtcTimestamp,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

/// Typed lifecycle planner output. Construction is engine-private so injected planners cannot bypass
/// the composed backend's exact-envelope validation.
pub struct AsyncLifecyclePlan {
    request: RawCommitRequest,
    expected_finalize_outcomes: Option<Vec<crate::FinalizeOutcome>>,
}

impl AsyncLifecyclePlan {
    pub(crate) fn renew(request: RawCommitRequest) -> Self {
        Self {
            request,
            expected_finalize_outcomes: None,
        }
    }

    pub(crate) fn reassign(request: RawCommitRequest) -> Self {
        Self {
            request,
            expected_finalize_outcomes: None,
        }
    }

    pub(crate) fn finalize(
        request: RawCommitRequest,
        expected_outcomes: Vec<crate::FinalizeOutcome>,
    ) -> Self {
        Self {
            request,
            expected_finalize_outcomes: Some(expected_outcomes),
        }
    }

    pub(crate) fn purge(request: RawCommitRequest) -> Self {
        Self {
            request,
            expected_finalize_outcomes: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn request(&self) -> &RawCommitRequest {
        &self.request
    }
}

/// Construction-injected preparation for typed lifecycle mutations. It owns validation and envelope
/// construction, but never durable commit authority.
pub trait AsyncLifecyclePlanner: Send + Sync + 'static {
    /// Resolve item-id lifecycle targets inside the same queue permit that owns planning and commit.
    fn resolve_lease_targets(
        &self,
        _shard: QueueKey,
        _item_ids: Vec<ItemId>,
    ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
        Box::pin(async { Err(EngineError::Unavailable) })
    }

    fn plan_renew(&self, request: AsyncRenewRequest)
    -> OwnedTask<EngineResult<AsyncLifecyclePlan>>;
    fn plan_reassign(
        &self,
        _request: AsyncReassignRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        Box::pin(async { Err(EngineError::Unavailable) })
    }
    fn plan_finalize(
        &self,
        _request: AsyncFinalizeRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        Box::pin(async { Err(EngineError::Unavailable) })
    }
    fn plan_purge(
        &self,
        _request: AsyncPurgeRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        Box::pin(async { Err(EngineError::Unavailable) })
    }
}

/// Marker used when typed lifecycle mutations were not injected.
pub struct NoAsyncLifecyclePlanner;

/// Marker used when typed expired-lease reclaim was not injected.
pub struct NoAsyncReclaimPlanner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncLifecyclePostCommitStage {
    CommitOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncLifecycleError {
    Submit(AsyncCommitSubmitError),
    BeforeCommit(EngineError),
    Commit(EngineError),
    AfterCommit {
        stage: AsyncLifecyclePostCommitStage,
        source: EngineError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncPushPostCommitStage {
    CommitOutcome,
}

/// Phase-aware failure for typed async push. A `Commit` error can be an unknown durable outcome;
/// callers with a request ID resolve it by retrying the same request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncPushError {
    Submit(AsyncCommitSubmitError),
    BeforeCommit(EngineError),
    Commit(EngineError),
    AfterCommit {
        stage: AsyncPushPostCommitStage,
        source: EngineError,
    },
}

/// An ADR-017 mutation-path scaffold, not a full backend implementation.
pub struct AsyncComposedBackend<
    S,
    D,
    P = NoAsyncClaimPlanner,
    U = NoAsyncPushPlanner,
    V = NoAsyncLifecyclePlanner,
    R = NoAsyncReclaimPlanner,
    C = NoAsyncCohortLifecyclePlanner,
> {
    strategy: Arc<S>,
    dispatcher: D,
    claim_planner: Arc<P>,
    push_planner: Arc<U>,
    lifecycle_planner: Arc<V>,
    reclaim_planner: Arc<R>,
    cohort_lifecycle_planner: Arc<C>,
    admission: KeyedQueueGate<crate::QueueKey>,
    durability: DurabilityClass,
    append_admission: AppendAdmissionClass,
}

impl<S, D> AsyncComposedBackend<S, D, NoAsyncClaimPlanner>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher,
{
    pub fn new(strategy: S, dispatcher: D, max_queued_commits: usize) -> Self {
        let durability = strategy.durability_class();
        Self {
            strategy: Arc::new(strategy),
            dispatcher,
            claim_planner: Arc::new(NoAsyncClaimPlanner),
            push_planner: Arc::new(NoAsyncPushPlanner),
            lifecycle_planner: Arc::new(NoAsyncLifecyclePlanner),
            reclaim_planner: Arc::new(NoAsyncReclaimPlanner),
            cohort_lifecycle_planner: Arc::new(NoAsyncCohortLifecyclePlanner),
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
            append_admission: AppendAdmissionClass::NonDerived,
        }
    }
}

impl<S, D, P> AsyncComposedBackend<S, D, P>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher,
{
    pub fn new_with_claim_planner(
        strategy: S,
        dispatcher: D,
        claim_planner: P,
        max_queued_commits: usize,
    ) -> Self {
        let durability = strategy.durability_class();
        Self {
            strategy: Arc::new(strategy),
            dispatcher,
            claim_planner: Arc::new(claim_planner),
            push_planner: Arc::new(NoAsyncPushPlanner),
            lifecycle_planner: Arc::new(NoAsyncLifecyclePlanner),
            reclaim_planner: Arc::new(NoAsyncReclaimPlanner),
            cohort_lifecycle_planner: Arc::new(NoAsyncCohortLifecyclePlanner),
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
            append_admission: AppendAdmissionClass::NonDerived,
        }
    }
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher,
{
    pub fn durability_class(&self) -> DurabilityClass {
        self.durability
    }

    /// Shared commit strategy handle for multi-step plan+commit under [`Self::submit_operation`].
    ///
    /// Callers that must validate optimistically against the projection (instance fences, lease
    /// checks) and then append+apply without a TOCTOU window should:
    /// 1. `let strategy = backend.commit_strategy();`
    /// 2. `backend.submit_operation(queue, || { plan...; strategy.commit(request) })`
    ///
    /// Do **not** nest [`Self::submit_commit`] / [`Self::submit_operation`] for the same queue
    /// inside that body — admission is not re-entrant and will deadlock.
    pub fn commit_strategy(&self) -> Arc<S> {
        Arc::clone(&self.strategy)
    }

    /// Serialize and dispatch one complete queue-local operation.
    ///
    /// The factory is invoked only after dispatcher acceptance. Its owned task retains the queue permit
    /// across every phase it contains, such as validation, idempotency, claim planning, commit, and render.
    /// This is the public composition primitive used by claim/push/lifecycle and by
    /// `commit_transition` so instance-fence validation cannot race concurrent applies on the same
    /// shard (fireweed-5497780d). Nested `submit_operation` / `submit_commit` for the same queue
    /// deadlocks.
    pub async fn submit_operation<T, F>(
        &self,
        queue: QueueKey,
        operation: F,
    ) -> Result<T, AsyncCommitSubmitError>
    where
        T: Send + 'static,
        F: FnOnce() -> OwnedTask<T> + Send + 'static,
    {
        let permit = self
            .admission
            .acquire(queue)
            .await
            .map_err(AsyncCommitSubmitError::Admission)?;
        let outcome = self
            .dispatcher
            .submit(Box::new(move || {
                let operation = operation();
                Box::pin(async move {
                    let _permit = permit;
                    operation.await
                })
            }))
            .map_err(AsyncCommitSubmitError::Dispatch)?;
        outcome.await.map_err(AsyncCommitSubmitError::Outcome)
    }

    /// Typed raw-commit wrapper over [`Self::submit_operation`].
    ///
    /// Strategy `prepare` runs **outside** the queue permit; only `commit_prepared` is serialized.
    /// Prefer [`Self::submit_operation`] + [`Self::commit_strategy`] when pre-commit validation
    /// must observe the post-apply projection of prior same-shard commits (instance fences).
    pub async fn submit_commit(
        &self,
        request: RawCommitRequest,
    ) -> Result<S::Output, AsyncCommitSubmitError>
    where
        S: PreparedAsyncCommitStrategy<Request = RawCommitRequest>,
    {
        let request = request.with_append_admission(self.append_admission);
        let queue = request.shard().clone();
        let prepared = self
            .strategy
            .prepare(request)
            .await
            .map_err(AsyncCommitSubmitError::Prepare)?;
        let strategy = Arc::clone(&self.strategy);
        self.submit_operation(queue, move || strategy.commit_prepared(prepared))
            .await
    }

    pub fn close(&self) {
        self.admission.close();
        self.dispatcher.close();
    }

    pub fn is_closed(&self) -> bool {
        self.admission.is_closed() || self.dispatcher.is_closed()
    }

    pub async fn drain(&self) -> Result<(), TaskOutcomeError> {
        self.dispatcher.drain().await
    }

    pub async fn close_and_drain(&self) -> Result<(), TaskOutcomeError> {
        self.close();
        self.drain().await
    }
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest, Output = EngineResult<RawCommitOutcome>>,
    D: OwnedTaskDispatcher,
    R: AsyncReclaimPlanner,
{
    /// Reclaim one deterministic batch of expired ordinary leases under the same queue permit used by
    /// every other typed mutation. Once dispatched, caller cancellation discards only the response.
    pub async fn reclaim_expired(
        &self,
        request: AsyncReclaimRequest,
    ) -> Result<Vec<ItemId>, AsyncLifecycleError> {
        let queue = request.shard.clone();
        let planner = Arc::clone(&self.reclaim_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let plan = planner
                    .plan_reclaim(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let (commit, item_ids) = plan.into_parts();
                let Some(commit) = commit else {
                    if !item_ids.is_empty() {
                        return Err(LifecycleExecutionError::BeforeCommit(EngineError::Invalid(
                            "invalid empty async reclaim plan",
                        )));
                    }
                    return Ok(item_ids);
                };
                validate_reclaim_plan(&request, &commit, &item_ids)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = commit.expected_epoch();
                let outcome = strategy
                    .commit(commit.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)?;
                Ok(item_ids)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest, Output = EngineResult<RawCommitOutcome>>,
    D: OwnedTaskDispatcher,
    C: AsyncCohortLifecyclePlanner,
{
    pub async fn renew_cohort(
        &self,
        request: AsyncCohortRenewRequest,
    ) -> Result<(), AsyncLifecycleError> {
        let queue = request.shard.clone();
        let planner = Arc::clone(&self.cohort_lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let plan = planner
                    .plan_cohort_renew(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                validate_cohort_renew_plan(&request, &plan.request, &plan.item_ids)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    pub async fn finalize_cohort(
        &self,
        request: AsyncCohortFinalizeRequest,
    ) -> Result<(), AsyncLifecycleError> {
        let queue = request.shard.clone();
        let planner = Arc::clone(&self.cohort_lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let plan = planner
                    .plan_cohort_finalize(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let outcomes = plan.outcomes.as_deref().ok_or({
                    LifecycleExecutionError::BeforeCommit(EngineError::Invalid(
                        "cohort finalize plan omitted sealed outcomes",
                    ))
                })?;
                validate_cohort_finalize_plan(&request, &plan.request, &plan.item_ids, outcomes)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }
}

fn validate_cohort_envelope(
    shard: &QueueKey,
    expected_epoch: Option<u64>,
    now: UtcTimestamp,
    commit: &RawCommitRequest,
) -> EngineResult<()> {
    if commit.shard() != shard
        || expected_epoch.is_some_and(|epoch| epoch != commit.expected_epoch())
        || commit.fault() != RawCommitFault::None
        || commit.commands().len() != 1
    {
        return Err(EngineError::Invalid("invalid async cohort lifecycle plan"));
    }
    let envelope = &commit.commands()[0];
    let unique: HashSet<_> = envelope.item_ids.iter().copied().collect();
    if envelope.item_ids.is_empty()
        || unique.len() != envelope.item_ids.len()
        || envelope.command_id.0.is_empty()
        || envelope.created_at != now
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
    {
        return Err(EngineError::Invalid("invalid async cohort lifecycle plan"));
    }
    Ok(())
}

fn validate_cohort_renew_plan(
    requested: &AsyncCohortRenewRequest,
    commit: &RawCommitRequest,
    sealed_ids: &[ItemId],
) -> EngineResult<()> {
    validate_cohort_envelope(
        &requested.shard,
        requested.expected_epoch,
        requested.now,
        commit,
    )?;
    let QueueCommand::CohortRenewLease(command) = &commit.commands()[0].command else {
        return Err(EngineError::Invalid("invalid async cohort renew plan"));
    };
    if sealed_ids != commit.commands()[0].item_ids
        || command.cohort_id != requested.cohort_id
        || command.lease_expires_at != requested.new_lease_expires_at
    {
        return Err(EngineError::Invalid("invalid async cohort renew plan"));
    }
    Ok(())
}

fn validate_cohort_finalize_plan(
    requested: &AsyncCohortFinalizeRequest,
    commit: &RawCommitRequest,
    sealed_ids: &[ItemId],
    sealed_outcomes: &[crate::FinalizeOutcome],
) -> EngineResult<()> {
    validate_cohort_envelope(
        &requested.shard,
        requested.expected_epoch,
        requested.now,
        commit,
    )?;
    let QueueCommand::CohortFinalize(command) = &commit.commands()[0].command else {
        return Err(EngineError::Invalid("invalid async cohort finalize plan"));
    };
    let outcome_ids = sealed_outcomes
        .iter()
        .map(|outcome| outcome.item_id)
        .collect::<Vec<_>>();
    let effective_kind = sealed_outcomes.first().map(|outcome| outcome.kind);
    let effective_not_before = sealed_outcomes
        .first()
        .and_then(|outcome| outcome.not_before);
    if sealed_ids != commit.commands()[0].item_ids
        || outcome_ids != sealed_ids
        || command.cohort_id != requested.cohort_id
        || Some(command.kind) != effective_kind
        || command.not_before != effective_not_before
        || sealed_outcomes.iter().any(|outcome| {
            Some(outcome.kind) != effective_kind
                || outcome.not_before != effective_not_before
                || match outcome.kind {
                    crate::FinalizeKind::Complete => {
                        outcome.applied_state != Some(fireweed_core::ItemState::Complete)
                    }
                    crate::FinalizeKind::Fail => {
                        outcome.applied_state != Some(fireweed_core::ItemState::Failed)
                    }
                    crate::FinalizeKind::Retry => !matches!(
                        outcome.applied_state,
                        Some(fireweed_core::ItemState::Pending | fireweed_core::ItemState::Failed)
                    ),
                    crate::FinalizeKind::Release => {
                        outcome.applied_state != Some(fireweed_core::ItemState::Pending)
                    }
                    crate::FinalizeKind::Rearm => true,
                }
        })
        || (command.kind != requested.kind
            && !(requested.kind == crate::FinalizeKind::Retry
                && command.kind == crate::FinalizeKind::Fail
                && command.not_before.is_none()))
        || (command.kind == requested.kind && command.not_before != requested.not_before)
    {
        return Err(EngineError::Invalid("invalid async cohort finalize plan"));
    }
    Ok(())
}

fn validate_reclaim_plan(
    requested: &AsyncReclaimRequest,
    commit: &RawCommitRequest,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    let unique: HashSet<_> = item_ids.iter().copied().collect();
    if item_ids.is_empty()
        || unique.len() != item_ids.len()
        || requested.limit.is_some_and(|limit| item_ids.len() > limit)
        || commit.shard() != &requested.shard
        || requested
            .expected_epoch
            .is_some_and(|epoch| epoch != commit.expected_epoch())
        || commit.commands().len() != 1
        || commit.fault() != RawCommitFault::None
    {
        return Err(EngineError::Invalid("invalid async reclaim plan"));
    }
    let envelope = &commit.commands()[0];
    let QueueCommand::LeaseExpired(command) = &envelope.command else {
        return Err(EngineError::Invalid("invalid async reclaim plan"));
    };
    if command.item_ids != item_ids
        || envelope.item_ids != item_ids
        || envelope.command_id.0.is_empty()
        || envelope.created_at != requested.now
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
    {
        return Err(EngineError::Invalid("invalid async reclaim plan"));
    }
    Ok(())
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest, Output = EngineResult<RawCommitOutcome>>,
    D: OwnedTaskDispatcher,
    U: AsyncPushPlanner,
{
    /// Validate, prepare, and durably commit a typed push under one queue-local permit.
    ///
    /// Once accepted by the dispatcher this operation is backend-owned: dropping the caller only loses
    /// the response. A commit-phase error may therefore represent an unknown outcome. Supplying a
    /// `request_id` lets the injected planner resolve that outcome on retry from retained replay state.
    pub async fn push(
        &self,
        request: AsyncPushRequest,
    ) -> Result<crate::PushBatchOutcome, AsyncPushError> {
        let queue = request.shard.clone();
        let strategy = Arc::clone(&self.strategy);
        let planner = Arc::clone(&self.push_planner);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                match planned_push(planner, request).await? {
                    PreparedPush::Replay(item_ids) => {
                        Ok::<crate::PushBatchOutcome, PushExecutionError>(
                            crate::PushBatchOutcome::replayed(item_ids),
                        )
                    }
                    PreparedPush::Commit { request, item_ids } => {
                        let expected_epoch = request.expected_epoch();
                        let outcome = strategy
                            .commit(request.with_append_admission(append_admission))
                            .await
                            .map_err(PushExecutionError::Commit)?;
                        validate_push_commit_outcome(&queue, expected_epoch, &outcome).map_err(
                            |source| PushExecutionError::AfterCommit {
                                stage: AsyncPushPostCommitStage::CommitOutcome,
                                source,
                            },
                        )?;
                        Ok(crate::PushBatchOutcome::fresh(item_ids))
                    }
                }
            })
        })
        .await
        .map_err(AsyncPushError::Submit)?
        .map_err(AsyncPushError::from)
    }

    /// Plan a push under the queue permit and release it before the caller appends.
    ///
    /// Object-log uses this so [`crate::AsyncCommitStrategy::commit`] can pack concurrent
    /// produces. Do not use on sqlite-log: releasing the permit before commit lets a second
    /// planner observe pre-apply state.
    pub async fn prepare_push(
        &self,
        request: AsyncPushRequest,
    ) -> Result<PreparedPush, AsyncPushError> {
        let planner = Arc::clone(&self.push_planner);
        self.submit_operation(request.shard.clone(), move || {
            Box::pin(async move { planned_push(planner, request).await })
        })
        .await
        .map_err(AsyncPushError::Submit)?
        .map_err(AsyncPushError::from)
    }
}

async fn planned_push<U: AsyncPushPlanner>(
    planner: Arc<U>,
    mut request: AsyncPushRequest,
) -> Result<PreparedPush, PushExecutionError> {
    let queue = request.shard.clone();
    let original_items = request.items.clone();
    canonicalize_push_gate_keys(&mut request.items);
    let definition = planner
        .queue_definition(queue.clone())
        .await
        .map_err(PushExecutionError::BeforeCommit)?;
    validate_push_definition(&queue, &request, &definition, planner.supports_gates())
        .map_err(PushExecutionError::BeforeCommit)?;
    let fingerprint = request
        .request_id
        .as_ref()
        .map(|_| {
            Ok(PushFingerprint {
                canonical_sha256: crate::push_specs_fingerprint_sha256(&request.items)?,
                legacy_body_hash: crate::compose::push_body_hash(&original_items)?,
            })
        })
        .transpose()
        .map_err(PushExecutionError::BeforeCommit)?;
    let plan = planner
        .plan_push(request.clone(), definition.clone(), fingerprint)
        .await
        .map_err(PushExecutionError::BeforeCommit)?;
    match plan.kind {
        AsyncPushPlanKind::Replay(item_ids) => {
            validate_push_replay(&request, &item_ids).map_err(PushExecutionError::BeforeCommit)?;
            Ok(PreparedPush::Replay(item_ids))
        }
        AsyncPushPlanKind::Commit {
            request: commit,
            item_ids,
        } => {
            validate_push_plan(&request, &definition, fingerprint, &commit, &item_ids)
                .map_err(PushExecutionError::BeforeCommit)?;
            Ok(PreparedPush::Commit {
                request: commit,
                item_ids,
            })
        }
    }
}

enum PushExecutionError {
    BeforeCommit(EngineError),
    Commit(EngineError),
    AfterCommit {
        stage: AsyncPushPostCommitStage,
        source: EngineError,
    },
}

impl From<PushExecutionError> for AsyncPushError {
    fn from(error: PushExecutionError) -> Self {
        match error {
            PushExecutionError::BeforeCommit(error) => Self::BeforeCommit(error),
            PushExecutionError::Commit(error) => Self::Commit(error),
            PushExecutionError::AfterCommit { stage, source } => {
                Self::AfterCommit { stage, source }
            }
        }
    }
}

fn validate_push_definition(
    queue: &QueueKey,
    request: &AsyncPushRequest,
    definition: &QueueDefinition,
    supports_gates: bool,
) -> EngineResult<()> {
    if definition.tenant_id != queue.tenant_id || definition.queue_id != queue.queue_id {
        return Err(EngineError::Storage(
            "async push planner returned the wrong queue definition".to_string(),
        ));
    }
    if request.items.is_empty() {
        // Empty batches are only meaningful under a request_id: a durable no-op with retained
        // replay/conflict semantics (e.g. snorri empty enqueue). Without a request_id there is
        // nothing to retain and no work to append.
        if request.request_id.is_none() {
            return Err(EngineError::Invalid("push batch must not be empty"));
        }
    }
    if request.items.len() > definition.max_push_batch_size as usize {
        return Err(EngineError::Invalid("push batch exceeds queue limit"));
    }
    validate_gate_push(supports_gates, &request.items)?;
    validate_push_shape(definition, &request.items)?;
    let schema = definition
        .entity_schema
        .as_ref()
        .and_then(|descriptor| descriptor.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()?;
    for item in &request.items {
        validate_entity(schema.as_ref(), item.entity.as_ref())?;
    }
    Ok(())
}

/// Validate priority, gate, cohort, and group shapes against one queue definition. Native adapters
/// reuse this before upsert append so replacement admission follows the same rules as ordinary push.
pub fn validate_push_shape(definition: &QueueDefinition, items: &[PushSpec]) -> EngineResult<()> {
    let mut request_gates = HashSet::new();
    let mut grouped_counts = std::collections::HashMap::new();
    for item in items {
        let priority_matches = matches!(
            (&definition.priority_model.kind, &item.priority),
            (_, None)
                | (
                    PriorityModelKind::Timestamp,
                    Some(PriorityValue::Timestamp(_))
                )
                | (PriorityModelKind::Int64, Some(PriorityValue::Int64(_)))
                | (PriorityModelKind::Decimal, Some(PriorityValue::Decimal(_)))
                | (PriorityModelKind::Text, Some(PriorityValue::Text(_)))
        );
        if !priority_matches {
            return Err(EngineError::Invalid("priority does not match queue model"));
        }
        if item.gate_keys.iter().any(|key| {
            key.is_empty()
                || key.len() > 256
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        }) {
            return Err(EngineError::Invalid("invalid gate key"));
        }
        if !item.gate_keys.is_empty()
            && definition.eligibility_policy.gate_keys != GateKeyPolicy::Dynamic
        {
            return Err(EngineError::Invalid("queue does not allow gate keys"));
        }
        if definition
            .eligibility_policy
            .max_gate_keys_per_item
            .is_some_and(|max| item.gate_keys.len() as u64 > max)
        {
            return Err(EngineError::Invalid("item gate-key cap exceeded"));
        }
        request_gates.extend(item.gate_keys.iter());
        let cohort_policy = definition.cohort_policy.filter(|policy| policy.enabled);
        match (&item.group_key, item.cohort_size, cohort_policy) {
            (None, _, Some(_)) | (_, None, Some(_)) => {
                return Err(EngineError::Invalid(
                    "cohort items require group_key and cohort_size",
                ));
            }
            (Some(_), Some(size), Some(policy)) => {
                if size == 0 || policy.max_cohort_size.is_some_and(|max| size > max) {
                    return Err(EngineError::Invalid("cohort size exceeds queue limit"));
                }
            }
            (_, Some(_), None) => {
                return Err(EngineError::Invalid("cohort_size is invalid on this queue"));
            }
            (Some(group), None, None) => {
                *grouped_counts.entry(group.as_str()).or_insert(0_u64) += 1;
            }
            (None, None, None) if definition.max_eligible_group_size.is_some() => {
                return Err(EngineError::Invalid(
                    "group batching items require group_key",
                ));
            }
            (None, None, None) => {}
        }
    }
    if definition
        .eligibility_policy
        .max_gates_per_request
        .is_some_and(|max| request_gates.len() as u64 > max)
    {
        return Err(EngineError::Invalid("request gate-key cap exceeded"));
    }
    if definition
        .max_eligible_group_size
        .is_some_and(|max| grouped_counts.values().any(|count| *count > max))
    {
        return Err(EngineError::Invalid("group batch exceeds queue limit"));
    }
    Ok(())
}

pub(crate) fn canonicalize_push_gate_keys(items: &mut [PushSpec]) {
    for item in items {
        item.gate_keys.sort();
        item.gate_keys.dedup();
    }
}

fn validate_push_replay(request: &AsyncPushRequest, item_ids: &[ItemId]) -> EngineResult<()> {
    let unique: HashSet<_> = item_ids.iter().copied().collect();
    if request.request_id.is_none()
        || item_ids.len() != request.items.len()
        || unique.len() != item_ids.len()
    {
        return Err(EngineError::Invalid("invalid async push replay"));
    }
    Ok(())
}

fn validate_push_plan(
    requested: &AsyncPushRequest,
    definition: &QueueDefinition,
    fingerprint: Option<PushFingerprint>,
    commit: &RawCommitRequest,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    let unique: HashSet<_> = item_ids.iter().copied().collect();
    if commit.shard() != &requested.shard
        || commit.fault() != RawCommitFault::None
        || requested
            .expected_epoch
            .is_some_and(|epoch| epoch != commit.expected_epoch())
        || item_ids.len() != requested.items.len()
        || unique.len() != item_ids.len()
        || item_ids
            .iter()
            .any(|item_id| item_id.epoch() != commit.expected_epoch())
        || commit.commands().len() != 1
    {
        return Err(EngineError::Invalid("invalid async push plan"));
    }
    let envelope = &commit.commands()[0];
    if envelope.item_ids != item_ids
        || envelope.command_id.0.is_empty()
        || envelope.checksum != CommandChecksum(0)
        || envelope.created_at != requested.now
        || envelope.request_id != requested.request_id
        || envelope.request_fingerprint != fingerprint.map(|hash| hash.legacy_body_hash.0)
        || envelope.request_outcome
            != requested.request_id.as_ref().map(|_| RequestOutcome::Push {
                item_ids: item_ids.to_vec(),
            })
    {
        return Err(EngineError::Invalid("invalid async push plan"));
    }
    let QueueCommand::Push(PushCommand { items }) = &envelope.command else {
        return Err(EngineError::Invalid("invalid async push plan"));
    };
    if items.len() != requested.items.len()
        || items
            .iter()
            .zip(&requested.items)
            .zip(item_ids)
            .any(|((planned, spec), item_id)| {
                !push_item_matches(planned, spec, *item_id, definition)
            })
    {
        return Err(EngineError::Invalid("invalid async push plan"));
    }
    Ok(())
}

fn push_item_matches(
    planned: &PushItem,
    spec: &PushSpec,
    item_id: ItemId,
    definition: &QueueDefinition,
) -> bool {
    let key_matches = spec.client_item_key.as_ref().map_or_else(
        || planned.client_item_key.as_str() == item_id.to_string(),
        |key| &planned.client_item_key == key,
    );
    // Admission (`admit_push_item_indexes`) materializes typed indexes from the spec and
    // may drop a fully-indexed entity document from the durable item; validate against the
    // admitted form, not the raw spec.
    //
    // `materialize_index_fields` returns the explicit map unchanged whenever it is
    // non-empty, so when the caller already passed explicit fields (the common case once
    // index fields are set on every item), borrow `spec.index_fields` directly instead of
    // cloning a potentially large map per item per commit just to hand it right back.
    let expected_index_fields: Cow<'_, BTreeMap<String, TypedValue>> =
        if spec.index_fields.is_empty() {
            match crate::index_fields::materialize_index_fields(
                definition,
                BTreeMap::new(),
                spec.entity.as_ref(),
            ) {
                Ok(map) => Cow::Owned(map),
                Err(_) => return false,
            }
        } else {
            Cow::Borrowed(&spec.index_fields)
        };
    let entity_dropped = crate::index_fields::entity_fully_indexed(
        definition,
        &expected_index_fields,
        spec.entity.as_ref(),
    );
    let expected_entity = if entity_dropped {
        None
    } else {
        spec.entity.clone()
    };
    planned.item_id == item_id
        && key_matches
        && planned.priority == spec.priority
        && planned.not_before == spec.not_before
        && planned.group_key == spec.group_key
        && planned.max_attempts == definition.retry_policy.max_attempts
        && planned.payload == spec.payload
        && planned.fields == spec.fields
        && planned.metadata == spec.metadata
        && planned.cohort_size == spec.cohort_size
        && planned.gate_keys == spec.gate_keys
        && planned.index_fields == *expected_index_fields
        && planned.entity_document == expected_entity
}

fn validate_push_commit_outcome(
    queue: &QueueKey,
    expected_epoch: u64,
    outcome: &RawCommitOutcome,
) -> EngineResult<()> {
    let positions = outcome.positions();
    if !outcome.projection_applied()
        || positions.len() != 1
        || positions[0].queue != *queue
        || positions[0].backend_epoch != expected_epoch
    {
        return Err(EngineError::Storage(
            "invalid async push commit outcome".to_string(),
        ));
    }
    Ok(())
}

enum LifecycleExecutionError {
    BeforeCommit(EngineError),
    Commit(EngineError),
    AfterCommit(EngineError),
}

impl From<LifecycleExecutionError> for AsyncLifecycleError {
    fn from(error: LifecycleExecutionError) -> Self {
        match error {
            LifecycleExecutionError::BeforeCommit(error) => Self::BeforeCommit(error),
            LifecycleExecutionError::Commit(error) => Self::Commit(error),
            LifecycleExecutionError::AfterCommit(source) => Self::AfterCommit {
                stage: AsyncLifecyclePostCommitStage::CommitOutcome,
                source,
            },
        }
    }
}

fn validate_renew_plan(
    requested: &AsyncRenewRequest,
    planned: &RawCommitRequest,
) -> EngineResult<()> {
    let requested_ids = requested
        .targets
        .iter()
        .map(|target| target.item_id)
        .collect::<Vec<_>>();
    let unique: HashSet<_> = requested_ids.iter().copied().collect();
    if requested_ids.is_empty() || unique.len() != requested_ids.len() {
        return Err(EngineError::Invalid("invalid renew item batch"));
    }
    if planned.shard() != &requested.shard
        || requested
            .expected_epoch
            .is_some_and(|epoch| epoch != planned.expected_epoch())
        || planned.commands().len() != 1
        || planned.fault() != RawCommitFault::None
    {
        return Err(EngineError::Invalid("invalid async renew plan"));
    }
    let envelope = &planned.commands()[0];
    let QueueCommand::RenewLease(command) = &envelope.command else {
        return Err(EngineError::Invalid("invalid async renew plan"));
    };
    if command.item_ids != requested_ids
        || command.lease_expires_at != requested.new_lease_expires_at
        || envelope.item_ids != requested_ids
        || envelope.command_id.0.is_empty()
        || envelope.created_at != requested.now
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
    {
        return Err(EngineError::Invalid("invalid async renew plan"));
    }
    Ok(())
}

fn validate_reassign_plan(
    requested: &AsyncReassignRequest,
    planned: &RawCommitRequest,
) -> EngineResult<()> {
    let requested_ids = requested
        .targets
        .iter()
        .map(|target| target.item_id)
        .collect::<Vec<_>>();
    let unique: HashSet<_> = requested_ids.iter().copied().collect();
    if requested_ids.is_empty() || unique.len() != requested_ids.len() {
        return Err(EngineError::Invalid("invalid reassign item batch"));
    }
    if planned.shard() != &requested.shard
        || requested
            .expected_epoch
            .is_some_and(|epoch| epoch != planned.expected_epoch())
        || planned.commands().len() != 1
        || planned.fault() != RawCommitFault::None
    {
        return Err(EngineError::Invalid("invalid async reassign plan"));
    }
    let envelope = &planned.commands()[0];
    let QueueCommand::ReassignLease(command) = &envelope.command else {
        return Err(EngineError::Invalid("invalid async reassign plan"));
    };
    if command.item_ids != requested_ids
        || command.lease_token != requested.new_lease_token
        || command.lease_expires_at != requested.new_lease_expires_at
        || envelope.item_ids != requested_ids
        || envelope.command_id.0.is_empty()
        || envelope.created_at != requested.now
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
    {
        return Err(EngineError::Invalid("invalid async reassign plan"));
    }
    Ok(())
}

fn validate_lifecycle_commit_outcome(
    durability: DurabilityClass,
    queue: &QueueKey,
    expected_epoch: u64,
    outcome: &RawCommitOutcome,
) -> EngineResult<()> {
    let positions = outcome.positions();
    if (durability == DurabilityClass::Atomic && !outcome.projection_applied())
        || positions.len() != 1
        || positions[0].queue != *queue
        || positions[0].backend_epoch != expected_epoch
    {
        return Err(EngineError::Storage(
            "invalid async lifecycle commit outcome".to_string(),
        ));
    }
    Ok(())
}

impl<S, D, P, U> AsyncComposedBackend<S, D, P, U, NoAsyncLifecyclePlanner>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher,
{
    pub fn new_with_planners(
        strategy: S,
        dispatcher: D,
        claim_planner: P,
        push_planner: U,
        max_queued_commits: usize,
    ) -> Self {
        let durability = strategy.durability_class();
        Self {
            strategy: Arc::new(strategy),
            dispatcher,
            claim_planner: Arc::new(claim_planner),
            push_planner: Arc::new(push_planner),
            lifecycle_planner: Arc::new(NoAsyncLifecyclePlanner),
            reclaim_planner: Arc::new(NoAsyncReclaimPlanner),
            cohort_lifecycle_planner: Arc::new(NoAsyncCohortLifecyclePlanner),
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
            append_admission: AppendAdmissionClass::NonDerived,
        }
    }
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C> {
    /// Add a separately injected lifecycle capability while preserving the existing claim/push profile.
    pub fn with_lifecycle_planner<W>(
        self,
        lifecycle_planner: W,
    ) -> AsyncComposedBackend<S, D, P, U, W, R, C> {
        AsyncComposedBackend {
            strategy: self.strategy,
            dispatcher: self.dispatcher,
            claim_planner: self.claim_planner,
            push_planner: self.push_planner,
            lifecycle_planner: Arc::new(lifecycle_planner),
            reclaim_planner: self.reclaim_planner,
            cohort_lifecycle_planner: self.cohort_lifecycle_planner,
            admission: self.admission,
            durability: self.durability,
            append_admission: self.append_admission,
        }
    }

    pub fn with_reclaim_planner<W>(
        self,
        reclaim_planner: W,
    ) -> AsyncComposedBackend<S, D, P, U, V, W, C> {
        AsyncComposedBackend {
            strategy: self.strategy,
            dispatcher: self.dispatcher,
            claim_planner: self.claim_planner,
            push_planner: self.push_planner,
            lifecycle_planner: self.lifecycle_planner,
            reclaim_planner: Arc::new(reclaim_planner),
            cohort_lifecycle_planner: self.cohort_lifecycle_planner,
            admission: self.admission,
            durability: self.durability,
            append_admission: self.append_admission,
        }
    }

    pub fn with_cohort_lifecycle_planner<W>(
        self,
        cohort_lifecycle_planner: W,
    ) -> AsyncComposedBackend<S, D, P, U, V, R, W> {
        AsyncComposedBackend {
            strategy: self.strategy,
            dispatcher: self.dispatcher,
            claim_planner: self.claim_planner,
            push_planner: self.push_planner,
            lifecycle_planner: self.lifecycle_planner,
            reclaim_planner: self.reclaim_planner,
            cohort_lifecycle_planner: Arc::new(cohort_lifecycle_planner),
            admission: self.admission,
            durability: self.durability,
            append_admission: self.append_admission,
        }
    }

    /// Configure the inert provenance attached to commits executed under this backend's queue gate.
    /// Generic compositions retain [`AppendAdmissionClass::NonDerived`].
    pub fn with_append_admission(mut self, append_admission: AppendAdmissionClass) -> Self {
        self.append_admission = append_admission;
        self
    }
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest, Output = EngineResult<RawCommitOutcome>>,
    D: OwnedTaskDispatcher,
    V: AsyncLifecyclePlanner,
{
    /// Validate and durably renew ordinary item leases under one queue-local permit.
    ///
    /// The current API has no request ID, so a commit-boundary error remains an unknown outcome; caller
    /// cancellation cannot stop an accepted backend-owned operation.
    pub async fn renew(&self, request: AsyncRenewRequest) -> Result<(), AsyncLifecycleError> {
        let queue = request.shard.clone();
        let planner = Arc::clone(&self.lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let plan = planner
                    .plan_renew(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                validate_renew_plan(&request, &plan.request)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    pub async fn finalize(&self, request: AsyncFinalizeRequest) -> Result<(), AsyncLifecycleError> {
        let queue = request.shard.clone();
        let planner = Arc::clone(&self.lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let plan = planner
                    .plan_finalize(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                validate_finalize_plan(
                    &request,
                    &plan.request,
                    plan.expected_finalize_outcomes.as_deref(),
                )
                .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    /// Finalize by item id/kind under one queue permit: render leases, validate, and commit together.
    ///
    /// Product adapters must use this instead of rendering `ClaimedItem` lease tokens *outside*
    /// [`Self::submit_operation`] then calling [`Self::finalize`] — that TOCTOU window is the
    /// fireweed-c8e0a7a5 / fireweed-2be744bd validate-before-apply family (snorri objectlog + worker pool).
    pub async fn finalize_outcomes(
        &self,
        shard: QueueKey,
        outcomes: Vec<crate::FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> Result<(), AsyncLifecycleError> {
        if outcomes.is_empty() {
            return Err(AsyncLifecycleError::BeforeCommit(EngineError::Invalid(
                "finalize item batch must not be empty",
            )));
        }
        let queue = shard.clone();
        let lifecycle = Arc::clone(&self.lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let prepared =
                    planned_finalize(lifecycle, shard, outcomes, now, expected_epoch).await?;
                let epoch = prepared.request.expected_epoch();
                let outcome = strategy
                    .commit(prepared.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    /// Plan a finalize under the queue permit and release it before the caller appends.
    ///
    /// Same sqlite-log restriction as [`Self::prepare_push`].
    pub async fn prepare_finalize(
        &self,
        shard: QueueKey,
        outcomes: Vec<crate::FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> Result<PreparedFinalize, AsyncLifecycleError> {
        if outcomes.is_empty() {
            return Err(AsyncLifecycleError::BeforeCommit(EngineError::Invalid(
                "finalize item batch must not be empty",
            )));
        }
        let lifecycle = Arc::clone(&self.lifecycle_planner);
        self.submit_operation(shard.clone(), move || {
            Box::pin(async move {
                planned_finalize(lifecycle, shard, outcomes, now, expected_epoch).await
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    /// Renew by item id under one queue permit: render leases, validate, and commit together.
    ///
    /// See [`Self::finalize_outcomes`] for the TOCTOU rationale (fireweed-c8e0a7a5).
    pub async fn renew_item_ids(
        &self,
        shard: QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> Result<(), AsyncLifecycleError> {
        if item_ids.is_empty() {
            return Err(AsyncLifecycleError::BeforeCommit(EngineError::Invalid(
                "renew item batch must not be empty",
            )));
        }
        let queue = shard.clone();
        let lifecycle = Arc::clone(&self.lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let claimed = lifecycle
                    .resolve_lease_targets(shard.clone(), item_ids.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                if claimed.len() != item_ids.len() {
                    return Err(LifecycleExecutionError::BeforeCommit(
                        EngineError::StaleLease,
                    ));
                }
                let targets = claimed
                    .into_iter()
                    .map(|item| {
                        Ok(RenewTarget {
                            item_id: item.item_id,
                            lease_token: item.lease_token.ok_or(EngineError::StaleLease)?,
                        })
                    })
                    .collect::<EngineResult<Vec<_>>>()
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let request = AsyncRenewRequest {
                    shard,
                    targets,
                    new_lease_expires_at,
                    now,
                    expected_epoch,
                };
                let plan = lifecycle
                    .plan_renew(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                validate_renew_plan(&request, &plan.request)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    /// Execute the internal async [`crate::PurgePort`] path and return its remove-if-present count.
    ///
    /// API adapters must resolve API-001 targeting and replay semantics before calling this method and
    /// must not treat this aggregate count as the public per-item response.
    pub async fn purge(&self, request: AsyncPurgeRequest) -> Result<u64, AsyncLifecycleError> {
        let queue = request.shard.clone();
        let planner = Arc::clone(&self.lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let plan = planner
                    .plan_purge(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let count = validate_purge_plan(&request, &plan.request)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                if count == 0 {
                    return Ok::<u64, LifecycleExecutionError>(count);
                }
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)?;
                Ok::<u64, LifecycleExecutionError>(count)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }

    /// Reassign item-id leases under one queue permit, preserving projection-owned rejection
    /// precedence before constructing the replacement lease command.
    pub async fn reassign_item_ids(
        &self,
        shard: QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> Result<(), AsyncLifecycleError> {
        if item_ids.is_empty() {
            return Err(AsyncLifecycleError::BeforeCommit(EngineError::Invalid(
                "reassign item batch must not be empty",
            )));
        }
        let queue = shard.clone();
        let lifecycle = Arc::clone(&self.lifecycle_planner);
        let strategy = Arc::clone(&self.strategy);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let claimed = lifecycle
                    .resolve_lease_targets(shard.clone(), item_ids.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                if claimed.len() != item_ids.len() {
                    return Err(LifecycleExecutionError::BeforeCommit(
                        EngineError::StaleLease,
                    ));
                }
                let targets = claimed
                    .into_iter()
                    .map(|item| {
                        Ok(RenewTarget {
                            item_id: item.item_id,
                            lease_token: item.lease_token.ok_or(EngineError::StaleLease)?,
                        })
                    })
                    .collect::<EngineResult<Vec<_>>>()
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let request = AsyncReassignRequest {
                    shard,
                    targets,
                    new_lease_token,
                    new_lease_expires_at,
                    now,
                    expected_epoch,
                };
                let plan = lifecycle
                    .plan_reassign(request.clone())
                    .await
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                validate_reassign_plan(&request, &plan.request)
                    .map_err(LifecycleExecutionError::BeforeCommit)?;
                let epoch = plan.request.expected_epoch();
                let outcome = strategy
                    .commit(plan.request.with_append_admission(append_admission))
                    .await
                    .map_err(LifecycleExecutionError::Commit)?;
                validate_lifecycle_commit_outcome(
                    strategy.durability_class(),
                    &queue,
                    epoch,
                    &outcome,
                )
                .map_err(LifecycleExecutionError::AfterCommit)
            })
        })
        .await
        .map_err(AsyncLifecycleError::Submit)?
        .map_err(AsyncLifecycleError::from)
    }
}

fn validate_purge_plan(
    request: &AsyncPurgeRequest,
    commit: &RawCommitRequest,
) -> EngineResult<u64> {
    if commit.shard() != &request.shard
        || commit.commands().len() != 1
        || commit.fault() != RawCommitFault::None
        || request
            .expected_epoch
            .is_some_and(|e| e != commit.expected_epoch())
    {
        return Err(EngineError::Invalid("invalid async purge plan"));
    }
    let env = &commit.commands()[0];
    let QueueCommand::PurgeItems(command) = &env.command else {
        return Err(EngineError::Invalid("invalid async purge plan"));
    };
    // Removing each planned ID validates membership and uniqueness together.
    // A linear request scan per ID makes large retention batches quadratic.
    let mut requested: HashSet<_> = request.item_ids.iter().copied().collect();
    if env.command_id.0.is_empty()
        || env.item_ids != command.item_ids
        || command.force != request.force
        || command.item_ids.iter().any(|id| !requested.remove(id))
        || env.created_at != request.now
        || env.request_id.is_some()
        || env.request_fingerprint.is_some()
        || env.request_outcome.is_some()
        || env.checksum != CommandChecksum(0)
    {
        return Err(EngineError::Invalid("invalid async purge plan"));
    }
    Ok(command.item_ids.len() as u64)
}

fn validate_finalize_plan(
    request: &AsyncFinalizeRequest,
    commit: &RawCommitRequest,
    expected_outcomes: Option<&[crate::FinalizeOutcome]>,
) -> EngineResult<()> {
    if commit.shard() != &request.shard
        || commit.commands().len() != 1
        || commit.fault() != RawCommitFault::None
        || request
            .expected_epoch
            .is_some_and(|epoch| epoch != commit.expected_epoch())
    {
        return Err(EngineError::Invalid("invalid async finalize plan"));
    }
    let envelope = &commit.commands()[0];
    let QueueCommand::Finalize(command) = &envelope.command else {
        return Err(EngineError::Invalid("invalid async finalize plan"));
    };
    let ids = request
        .targets
        .iter()
        .map(|target| target.item_id)
        .collect::<Vec<_>>();
    let unique: HashSet<_> = ids.iter().copied().collect();
    let Some(expected_outcomes) = expected_outcomes else {
        return Err(EngineError::Invalid("invalid async finalize plan"));
    };
    let exact_expected = command.outcomes.len() == expected_outcomes.len()
        && command
            .outcomes
            .iter()
            .zip(expected_outcomes)
            .all(|(actual, expected)| {
                actual.item_id == expected.item_id
                    && actual.kind == expected.kind
                    && actual.applied_state == expected.applied_state
                    && actual.not_before == expected.not_before
            });
    let outcomes_match = exact_expected
        && expected_outcomes.len() == request.targets.len()
        && expected_outcomes
            .iter()
            .zip(&request.targets)
            .all(|(outcome, target)| {
                let state_matches = matches!(
                    (target.kind, outcome.applied_state),
                    (
                        crate::FinalizeKind::Complete,
                        Some(fireweed_core::ItemState::Complete)
                    ) | (
                        crate::FinalizeKind::Fail,
                        Some(fireweed_core::ItemState::Failed)
                    ) | (
                        crate::FinalizeKind::Release | crate::FinalizeKind::Rearm,
                        Some(fireweed_core::ItemState::Pending),
                    ) | (
                        crate::FinalizeKind::Retry,
                        Some(fireweed_core::ItemState::Pending | fireweed_core::ItemState::Failed),
                    )
                );
                outcome.item_id == target.item_id
                    && outcome.kind == target.kind
                    && outcome.not_before == target.not_before
                    && state_matches
            });
    if ids.is_empty()
        || unique.len() != ids.len()
        || envelope.command_id.0.is_empty()
        || envelope.item_ids != ids
        || envelope.created_at != request.now
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
        || !outcomes_match
    {
        return Err(EngineError::Invalid("invalid async finalize plan"));
    }
    Ok(())
}

impl<S, D, P, U, V, R, C> AsyncComposedBackend<S, D, P, U, V, R, C>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest, Output = EngineResult<RawCommitOutcome>>,
    D: OwnedTaskDispatcher,
    P: AsyncClaimPlanner,
{
    /// Plan, durably commit, and render one typed claim under a single queue-local permit.
    pub async fn claim(&self, request: ClaimRequest) -> Result<Claimed, AsyncClaimError> {
        let queue = request.shard.clone();
        let strategy = Arc::clone(&self.strategy);
        let planner = Arc::clone(&self.claim_planner);
        let append_admission = self.append_admission;
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                match planned_claim(Arc::clone(&planner), request.clone()).await? {
                    PreparedClaim::Empty => Ok::<Claimed, ClaimExecutionError>(Claimed::default()),
                    PreparedClaim::Commit {
                        request: commit,
                        item_ids,
                        cohort_id,
                    } => {
                        let expected_epoch = commit.expected_epoch();
                        let outcome = strategy
                            .commit(commit.with_append_admission(append_admission))
                            .await
                            .map_err(ClaimExecutionError::Commit)?;
                        validate_claim_commit_outcome(&queue, expected_epoch, &outcome).map_err(
                            |source| ClaimExecutionError::AfterCommit {
                                stage: AsyncClaimPostCommitStage::CommitOutcome,
                                source,
                            },
                        )?;
                        finish_rendered_claim(planner, request, item_ids, cohort_id).await
                    }
                }
            })
        })
        .await
        .map_err(AsyncClaimError::Submit)?
        .map_err(AsyncClaimError::from)
    }

    /// Plan a claim under the queue permit and release it before the caller appends.
    ///
    /// Same sqlite-log restriction as [`Self::prepare_push`]. Object-log × Turso S5 uses
    /// [`Self::plan_exclusive_claim`] so the selection fence, not this keyed permit, spans
    /// caught-up selection through publication.
    pub async fn prepare_claim(
        &self,
        request: ClaimRequest,
    ) -> Result<PreparedClaim, AsyncClaimError> {
        let planner = Arc::clone(&self.claim_planner);
        self.submit_operation(request.shard.clone(), move || {
            Box::pin(async move { planned_claim(planner, request).await })
        })
        .await
        .map_err(AsyncClaimError::Submit)?
        .map_err(AsyncClaimError::from)
    }

    /// Plan one exclusive Claim without taking the keyed queue permit.
    ///
    /// The caller must already hold ClaimQueueTurn, the driver slot, and SelectionFenceAdmission.
    /// Item envelopes are stamped `authority_first`; grouped/cohort shaping stays with the
    /// retained carrier.
    pub async fn plan_exclusive_claim(
        &self,
        request: ClaimRequest,
    ) -> Result<PreparedClaim, AsyncClaimError> {
        planned_claim(Arc::clone(&self.claim_planner), request)
            .await
            .map(PreparedClaim::with_item_authority_first)
            .map_err(AsyncClaimError::from)
    }

    pub fn claim_planner(&self) -> Arc<P> {
        Arc::clone(&self.claim_planner)
    }

    /// Render a claim after the caller appended the prepared commit (object-log pack path).
    pub async fn render_prepared_claim(
        &self,
        request: ClaimRequest,
        item_ids: Vec<ItemId>,
        cohort_id: Option<CohortId>,
    ) -> Result<Claimed, AsyncClaimError> {
        finish_rendered_claim(
            Arc::clone(&self.claim_planner),
            request,
            item_ids,
            cohort_id,
        )
        .await
        .map_err(AsyncClaimError::from)
    }
}

async fn planned_finalize<V: AsyncLifecyclePlanner>(
    lifecycle: Arc<V>,
    shard: QueueKey,
    outcomes: Vec<crate::FinalizeOutcome>,
    now: UtcTimestamp,
    expected_epoch: Option<u64>,
) -> Result<PreparedFinalize, LifecycleExecutionError> {
    let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
    let claimed = lifecycle
        .resolve_lease_targets(shard.clone(), item_ids.clone())
        .await
        .map_err(LifecycleExecutionError::BeforeCommit)?;
    if claimed.len() != outcomes.len() {
        return Err(LifecycleExecutionError::BeforeCommit(
            EngineError::StaleLease,
        ));
    }
    let targets = outcomes
        .into_iter()
        .zip(claimed)
        .map(|(outcome, item)| {
            Ok(FinalizeTarget {
                item_id: outcome.item_id,
                lease_token: item.lease_token.ok_or(EngineError::StaleLease)?,
                item_version: item.item_version,
                kind: outcome.kind,
                not_before: outcome.not_before,
            })
        })
        .collect::<EngineResult<Vec<_>>>()
        .map_err(LifecycleExecutionError::BeforeCommit)?;
    let request = AsyncFinalizeRequest {
        shard,
        targets,
        now,
        expected_epoch,
    };
    let plan = lifecycle
        .plan_finalize(request.clone())
        .await
        .map_err(LifecycleExecutionError::BeforeCommit)?;
    validate_finalize_plan(
        &request,
        &plan.request,
        plan.expected_finalize_outcomes.as_deref(),
    )
    .map_err(LifecycleExecutionError::BeforeCommit)?;
    Ok(PreparedFinalize {
        request: plan.request,
        expected_finalize_outcomes: plan.expected_finalize_outcomes,
    })
}

async fn planned_claim<P: AsyncClaimPlanner>(
    planner: Arc<P>,
    request: ClaimRequest,
) -> Result<PreparedClaim, ClaimExecutionError> {
    let queue = request.shard.clone();
    let definition = planner
        .queue_definition(queue.clone())
        .await
        .map_err(ClaimExecutionError::BeforeCommit)?;
    if definition.tenant_id != queue.tenant_id || definition.queue_id != queue.queue_id {
        return Err(ClaimExecutionError::BeforeCommit(EngineError::Storage(
            "async claim planner returned the wrong queue definition".to_string(),
        )));
    }
    if request.max_items == 0 || request.max_items > definition.max_claim_batch_size as usize {
        return Err(ClaimExecutionError::BeforeCommit(EngineError::Invalid(
            "claim batch is outside queue limits",
        )));
    }
    let unit = validate_claim_compatibility(
        &request.compatibility,
        request.max_items as u64,
        &definition,
    )
    .map_err(ClaimExecutionError::BeforeCommit)?;
    let plan = planner
        .plan_claim(request.clone(), unit)
        .await
        .map_err(ClaimExecutionError::BeforeCommit)?;
    let AsyncClaimPlanKind::Commit {
        request: commit,
        item_ids,
        cohort_id,
    } = plan.kind
    else {
        return Ok(PreparedClaim::Empty);
    };
    validate_claim_plan(&request, &queue, &commit, &item_ids, cohort_id.as_ref())
        .map_err(ClaimExecutionError::BeforeCommit)?;
    Ok(PreparedClaim::Commit {
        request: commit,
        item_ids,
        cohort_id,
    })
}

async fn finish_rendered_claim<P: AsyncClaimPlanner>(
    planner: Arc<P>,
    request: ClaimRequest,
    item_ids: Vec<ItemId>,
    cohort_id: Option<CohortId>,
) -> Result<Claimed, ClaimExecutionError> {
    let queue = request.shard.clone();
    let items = planner
        .render_claimed(queue, item_ids.clone())
        .await
        .map_err(|source| ClaimExecutionError::AfterCommit {
            stage: AsyncClaimPostCommitStage::Render,
            source,
        })?;
    finish_retained_grouped_cohort_claim(&request, &item_ids, items, cohort_id).map_err(|source| {
        ClaimExecutionError::AfterCommit {
            stage: AsyncClaimPostCommitStage::RenderValidation,
            source,
        }
    })
}

/// Finish a grouped/cohort Claim from already-materialized full rows.
///
/// Preserves [`validate_rendered_claim`] count/order and per-item token/expiry equality, then
/// shapes a whole-cohort envelope by stripping per-item tokens. Post-apply continuation uses the
/// returned [`Claimed`] and needs no projection handle.
pub fn finish_retained_grouped_cohort_claim(
    request: &ClaimRequest,
    item_ids: &[ItemId],
    mut items: Vec<ClaimedItem>,
    cohort_id: Option<CohortId>,
) -> EngineResult<Claimed> {
    validate_rendered_claim(request, item_ids, &items)?;
    let mut claimed = Claimed {
        items: Vec::new(),
        cohort_lease_token: None,
        cohort_id: None,
    };
    if let Some(cohort_id) = cohort_id {
        for item in &mut items {
            item.lease_token = None;
        }
        claimed.cohort_lease_token = Some(request.lease_token.clone());
        claimed.cohort_id = Some(cohort_id);
    }
    claimed.items = items;
    Ok(claimed)
}

enum ClaimExecutionError {
    BeforeCommit(EngineError),
    Commit(EngineError),
    AfterCommit {
        stage: AsyncClaimPostCommitStage,
        source: EngineError,
    },
}

impl From<ClaimExecutionError> for AsyncClaimError {
    fn from(error: ClaimExecutionError) -> Self {
        match error {
            ClaimExecutionError::BeforeCommit(error) => Self::BeforeCommit(error),
            ClaimExecutionError::Commit(error) => Self::Commit(error),
            ClaimExecutionError::AfterCommit { stage, source } => {
                Self::AfterCommit { stage, source }
            }
        }
    }
}

fn validate_claim_plan(
    requested: &ClaimRequest,
    queue: &QueueKey,
    commit: &RawCommitRequest,
    item_ids: &[ItemId],
    cohort_id: Option<&CohortId>,
) -> EngineResult<()> {
    let unique_ids: HashSet<_> = item_ids.iter().copied().collect();
    if commit.shard() != queue
        || commit.fault() != RawCommitFault::None
        || item_ids.is_empty()
        || item_ids.len() > requested.max_items
        || unique_ids.len() != item_ids.len()
        || cohort_id.is_some() != requested.compatibility.whole_cohort
        || (requested.compatibility.whole_cohort
            && (requested.compatibility.same_group_key
                || requested.compatibility.group_key.is_some()
                || requested.compatibility.group_batching.is_some()))
        || requested
            .expected_epoch
            .is_some_and(|epoch| epoch != commit.expected_epoch())
        || commit.commands().len() != 1
    {
        return Err(EngineError::Invalid("invalid async claim plan"));
    }
    let envelope = &commit.commands()[0];
    if envelope.item_ids != item_ids
        || envelope.command_id.0.is_empty()
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
        || envelope.created_at != requested.now
    {
        return Err(EngineError::Invalid("invalid async claim plan"));
    }
    let valid = match (&envelope.command, cohort_id) {
        (QueueCommand::Claim(command), None) => claim_command_matches(command, requested, item_ids),
        (QueueCommand::CohortClaim(command), Some(cohort_id)) => {
            cohort_claim_command_matches(command, requested, item_ids, cohort_id)
        }
        _ => false,
    };
    if !valid {
        return Err(EngineError::Invalid("invalid async claim plan"));
    }
    Ok(())
}

fn validate_claim_commit_outcome(
    queue: &QueueKey,
    expected_epoch: u64,
    outcome: &RawCommitOutcome,
) -> EngineResult<()> {
    let positions = outcome.positions();
    if !outcome.projection_applied()
        || positions.len() != 1
        || positions[0].queue != *queue
        || positions[0].backend_epoch != expected_epoch
    {
        return Err(EngineError::Storage(
            "invalid async claim commit outcome".to_string(),
        ));
    }
    Ok(())
}

fn validate_rendered_claim(
    requested: &ClaimRequest,
    item_ids: &[ItemId],
    items: &[ClaimedItem],
) -> EngineResult<()> {
    if items.len() != item_ids.len()
        || items.iter().zip(item_ids).any(|(item, expected_id)| {
            item.item_id != *expected_id
                || item.lease_token.as_ref() != Some(&requested.lease_token)
                || item.lease_expires_at != requested.lease_expires_at
        })
    {
        return Err(EngineError::Storage(
            "invalid async claim render".to_string(),
        ));
    }
    Ok(())
}

fn claim_command_matches(
    command: &ClaimCommand,
    requested: &ClaimRequest,
    item_ids: &[ItemId],
) -> bool {
    command.item_ids == item_ids
        && command.lease_token == requested.lease_token
        && command.lease_expires_at == requested.lease_expires_at
        && command.worker_id.as_ref() == Some(&requested.worker_id)
}

fn cohort_claim_command_matches(
    command: &CohortClaimCommand,
    requested: &ClaimRequest,
    item_ids: &[ItemId],
    cohort_id: &CohortId,
) -> bool {
    command.cohort_id == *cohort_id
        && command.item_ids == item_ids
        && command.lease_token == requested.lease_token
        && command.lease_expires_at == requested.lease_expires_at
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Instant;

    use std::collections::BTreeMap;

    use fireweed_core::{
        BodyHash, ClientItemKey, CohortId, EligibilityPolicy, GroupKey, ItemId, ItemState,
        LeaseToken, Metadata, MetadataValue, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
        RecurrencePolicy, RequestId, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };

    use super::*;
    use crate::{
        AsyncReclaimPlan, ClaimCommand, ClaimCompatibility, CommandChecksum, CommandEnvelope,
        CommandId, CommandPosition, FinalizeCommand, FinalizeKind, FinalizeOutcome, OwnedTask,
        OwnedTaskFactory, QueueCommand, QueueKey, RawCommitOutcome, RequestOutcome, TaskOutcome,
        TaskOutcomeSender, UnifiedAtomicCommit, UnifiedAtomicCommitter, build_push_items,
        task_outcome_channel,
    };

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    trait ErasedTask: Send {
        fn poll_erased(&mut self, context: &mut Context<'_>) -> Poll<()>;
    }

    struct TypedTask<T> {
        task: OwnedTask<T>,
        outcome: Option<TaskOutcomeSender<T>>,
    }

    impl<T: Send + 'static> ErasedTask for TypedTask<T> {
        fn poll_erased(&mut self, context: &mut Context<'_>) -> Poll<()> {
            match self.task.as_mut().poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(value) => {
                    self.outcome
                        .take()
                        .expect("typed task outcome sender missing")
                        .send(value);
                    Poll::Ready(())
                }
            }
        }
    }

    struct DispatchState {
        closed: bool,
        capacity: usize,
        next_id: u64,
        accepted: usize,
        live: HashSet<u64>,
        ready: VecDeque<u64>,
        tasks: HashMap<u64, Box<dyn ErasedTask>>,
        drainers: Vec<TaskOutcomeSender<()>>,
    }

    struct DispatchInner {
        state: Mutex<DispatchState>,
    }

    #[derive(Clone)]
    struct ControlledDispatcher {
        inner: Arc<DispatchInner>,
    }

    struct TaskWake {
        id: u64,
        dispatcher: Weak<DispatchInner>,
    }

    impl Wake for TaskWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if let Some(dispatcher) = self.dispatcher.upgrade() {
                let mut state = dispatcher.state.lock().unwrap();
                if state.live.contains(&self.id) {
                    state.ready.push_back(self.id);
                }
            }
        }
    }

    impl ControlledDispatcher {
        fn new(capacity: usize) -> Self {
            Self {
                inner: Arc::new(DispatchInner {
                    state: Mutex::new(DispatchState {
                        closed: false,
                        capacity,
                        next_id: 0,
                        accepted: 0,
                        live: HashSet::new(),
                        ready: VecDeque::new(),
                        tasks: HashMap::new(),
                        drainers: Vec::new(),
                    }),
                }),
            }
        }

        fn accepted(&self) -> usize {
            self.inner.state.lock().unwrap().accepted
        }

        fn drive_next(&self) -> bool {
            let (id, mut slot) = loop {
                let candidate = {
                    let mut state = self.inner.state.lock().unwrap();
                    state
                        .ready
                        .pop_front()
                        .and_then(|id| state.tasks.remove(&id).map(|slot| (id, slot)))
                };
                if let Some(candidate) = candidate {
                    break candidate;
                }
                if self.inner.state.lock().unwrap().ready.is_empty() {
                    return false;
                }
            };
            let waker = Waker::from(Arc::new(TaskWake {
                id,
                dispatcher: Arc::downgrade(&self.inner),
            }));
            match slot.poll_erased(&mut Context::from_waker(&waker)) {
                Poll::Pending => {
                    self.inner.state.lock().unwrap().tasks.insert(id, slot);
                }
                Poll::Ready(()) => {
                    let drainers = {
                        let mut state = self.inner.state.lock().unwrap();
                        state.live.remove(&id);
                        state.accepted -= 1;
                        if state.closed && state.accepted == 0 {
                            std::mem::take(&mut state.drainers)
                        } else {
                            Vec::new()
                        }
                    };
                    for drainer in drainers {
                        drainer.send(());
                    }
                }
            }
            true
        }
    }

    impl OwnedTaskDispatcher for ControlledDispatcher {
        fn submit<T: Send + 'static>(
            &self,
            factory: OwnedTaskFactory<T>,
        ) -> Result<TaskOutcome<T>, DispatchError> {
            let id = {
                let mut state = self.inner.state.lock().unwrap();
                if state.closed {
                    return Err(DispatchError::Closed);
                }
                if state.accepted >= state.capacity {
                    return Err(DispatchError::AtCapacity);
                }
                let id = state.next_id;
                state.next_id = state.next_id.wrapping_add(1);
                state.accepted += 1;
                state.live.insert(id);
                id
            };
            let task = factory();
            let (outcome_sender, outcome) = task_outcome_channel();
            let mut state = self.inner.state.lock().unwrap();
            state.tasks.insert(
                id,
                Box::new(TypedTask {
                    task,
                    outcome: Some(outcome_sender),
                }),
            );
            state.ready.push_back(id);
            Ok(outcome)
        }

        fn close(&self) {
            let drainers = {
                let mut state = self.inner.state.lock().unwrap();
                state.closed = true;
                if state.accepted == 0 {
                    std::mem::take(&mut state.drainers)
                } else {
                    Vec::new()
                }
            };
            for drainer in drainers {
                drainer.send(());
            }
        }

        fn is_closed(&self) -> bool {
            self.inner.state.lock().unwrap().closed
        }

        fn drain(&self) -> TaskOutcome<()> {
            let (sender, outcome) = task_outcome_channel();
            let immediate = {
                let mut state = self.inner.state.lock().unwrap();
                if state.closed && state.accepted == 0 {
                    Some(sender)
                } else {
                    state.drainers.push(sender);
                    None
                }
            };
            if let Some(sender) = immediate {
                sender.send(());
            }
            outcome
        }
    }

    struct Phase {
        started: AtomicBool,
        released: AtomicBool,
        waker: Mutex<Option<Waker>>,
    }

    impl Phase {
        fn new(released: bool) -> Arc<Self> {
            Arc::new(Self {
                started: AtomicBool::new(false),
                released: AtomicBool::new(released),
                waker: Mutex::new(None),
            })
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    #[derive(Clone)]
    struct ControlledCommitter {
        constructed: Arc<AtomicUsize>,
        phases: Arc<Mutex<HashMap<QueueKey, Arc<Phase>>>>,
        completed: Arc<Mutex<Vec<QueueKey>>>,
    }

    impl UnifiedAtomicCommitter for ControlledCommitter {
        type Request = RawCommitRequest;
        type Output = QueueKey;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            self.constructed.fetch_add(1, Ordering::AcqRel);
            let key = request.shard().clone();
            let phase = Arc::clone(self.phases.lock().unwrap().get(&key).unwrap());
            let completed = Arc::clone(&self.completed);
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if phase.released.load(Ordering::Acquire) {
                    completed.lock().unwrap().push(key.clone());
                    Poll::Ready(key.clone())
                } else {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }
    }

    fn queue(name: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn request(name: &str) -> RawCommitRequest {
        RawCommitRequest::new(queue(name), Vec::new(), 1)
    }

    fn claim_request(name: &str) -> ClaimRequest {
        ClaimRequest {
            shard: queue(name),
            worker_id: WorkerId::new("worker").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new(format!("lease-{name}")).unwrap(),
            lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
            now: UtcTimestamp::new(10, 0).unwrap(),
            eligibility_time: None,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(1),
        }
    }

    fn push_request(name: &str, with_request_id: bool) -> AsyncPushRequest {
        AsyncPushRequest {
            shard: queue(name),
            request_id: with_request_id.then(|| RequestId::new(format!("request-{name}")).unwrap()),
            items: vec![PushSpec {
                payload: Some(bytes::Bytes::from_static(b"payload")),
                ..PushSpec::default()
            }],
            now: UtcTimestamp::new(10, 0).unwrap(),
            expected_epoch: Some(1),
        }
    }

    fn definition(name: &str) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new(name).unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn claim_envelope(request: &ClaimRequest, item_id: ItemId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(format!("claim-{item_id}")),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None::<RequestOutcome>,
            item_ids: vec![item_id],
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token: request.lease_token.clone(),
                lease_expires_at: request.lease_expires_at,
                worker_id: Some(request.worker_id.clone()),
                authority_first: false,
            }),
            checksum: CommandChecksum(0),
            created_at: request.now,
        }
    }

    fn claimed_item(request: &ClaimRequest, item_id: ItemId) -> ClaimedItem {
        ClaimedItem {
            item_id,
            client_item_key: ClientItemKey::new(format!("key-{item_id}")).unwrap(),
            item_version: 1,
            priority: None,
            group_key: None,
            not_before: None,
            lease_token: Some(request.lease_token.clone()),
            lease_expires_at: request.lease_expires_at,
            attempt_count: 1,
            max_attempts: 3,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            gate_keys: Vec::new(),
            entity: None,
        }
    }

    type TestBackend =
        AsyncComposedBackend<UnifiedAtomicCommit<ControlledCommitter>, ControlledDispatcher>;

    struct Fixture {
        backend: TestBackend,
        dispatcher: ControlledDispatcher,
        constructed: Arc<AtomicUsize>,
        completed: Arc<Mutex<Vec<QueueKey>>>,
        phases: Arc<Mutex<HashMap<QueueKey, Arc<Phase>>>>,
    }

    fn fixture(capacity: usize) -> Fixture {
        let constructed = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(Mutex::new(Vec::new()));
        let phases = Arc::new(Mutex::new(HashMap::new()));
        let committer = ControlledCommitter {
            constructed: Arc::clone(&constructed),
            phases: Arc::clone(&phases),
            completed: Arc::clone(&completed),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        Fixture {
            backend: AsyncComposedBackend::new(strategy, dispatcher.clone(), 8),
            dispatcher,
            constructed,
            completed,
            phases,
        }
    }

    #[derive(Clone)]
    struct ClaimCommitter {
        calls: Arc<AtomicUsize>,
        completed: Arc<AtomicBool>,
        phase: Arc<Phase>,
    }

    impl UnifiedAtomicCommitter for ClaimCommitter {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let completed = Arc::clone(&self.completed);
            let phase = Arc::clone(&self.phase);
            let queue = request.shard().clone();
            let epoch = request.expected_epoch();
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if !phase.released.load(Ordering::Acquire) {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    return Poll::Pending;
                }
                completed.store(true, Ordering::Release);
                Poll::Ready(Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                    queue.clone(),
                    epoch,
                    1,
                )])))
            }))
        }
    }

    #[derive(Clone)]
    struct ControlledClaimPlanner {
        item_id: ItemId,
        plan_calls: Arc<AtomicUsize>,
        render_calls: Arc<AtomicUsize>,
        commit_completed: Arc<AtomicBool>,
        render_phase: Arc<Phase>,
    }

    impl AsyncClaimPlanner for ControlledClaimPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            self.plan_calls.fetch_add(1, Ordering::AcqRel);
            let item_id = self.item_id;
            Box::pin(async move {
                let envelope = claim_envelope(&request, item_id);
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(request.shard.clone(), vec![envelope], 1),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            shard: QueueKey,
            item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            assert!(
                self.commit_completed.load(Ordering::Acquire),
                "claim rendering must follow the strategy commit"
            );
            self.render_calls.fetch_add(1, Ordering::AcqRel);
            let phase = Arc::clone(&self.render_phase);
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if !phase.released.load(Ordering::Acquire) {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    return Poll::Pending;
                }
                let request = claim_request(shard.queue_id.as_str());
                Poll::Ready(Ok(item_ids
                    .iter()
                    .copied()
                    .map(|id| claimed_item(&request, id))
                    .collect()))
            }))
        }
    }

    type ClaimBackend = AsyncComposedBackend<
        UnifiedAtomicCommit<ClaimCommitter>,
        ControlledDispatcher,
        ControlledClaimPlanner,
    >;

    struct ClaimFixture {
        backend: ClaimBackend,
        dispatcher: ControlledDispatcher,
        phase: Arc<Phase>,
        render_phase: Arc<Phase>,
        commit_calls: Arc<AtomicUsize>,
        commit_completed: Arc<AtomicBool>,
        plan_calls: Arc<AtomicUsize>,
        render_calls: Arc<AtomicUsize>,
        item_id: ItemId,
    }

    fn claim_fixture(capacity: usize, released: bool, render_released: bool) -> ClaimFixture {
        let phase = Phase::new(released);
        let render_phase = Phase::new(render_released);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let commit_completed = Arc::new(AtomicBool::new(false));
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let render_calls = Arc::new(AtomicUsize::new(0));
        let item_id = ItemId::mint(1, 1, 1);
        let committer = ClaimCommitter {
            calls: Arc::clone(&commit_calls),
            completed: Arc::clone(&commit_completed),
            phase: Arc::clone(&phase),
        };
        let planner = ControlledClaimPlanner {
            item_id,
            plan_calls: Arc::clone(&plan_calls),
            render_calls: Arc::clone(&render_calls),
            commit_completed: Arc::clone(&commit_completed),
            render_phase: Arc::clone(&render_phase),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        let backend =
            AsyncComposedBackend::new_with_claim_planner(strategy, dispatcher.clone(), planner, 8);
        ClaimFixture {
            backend,
            dispatcher,
            phase,
            render_phase,
            commit_calls,
            commit_completed,
            plan_calls,
            render_calls,
            item_id,
        }
    }

    #[derive(Clone, Copy)]
    enum PushPlanMode {
        Valid,
        Replay,
        SmugglePayload,
        WrongFingerprint,
    }

    #[derive(Clone)]
    struct ControlledPushPlanner {
        mode: PushPlanMode,
        item_id: ItemId,
        calls: Arc<AtomicUsize>,
    }

    impl AsyncPushPlanner for ControlledPushPlanner {
        fn supports_gates(&self) -> bool {
            false
        }

        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_push(
            &self,
            request: AsyncPushRequest,
            _definition: QueueDefinition,
            fingerprint: Option<PushFingerprint>,
        ) -> OwnedTask<EngineResult<AsyncPushPlan>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let mode = self.mode;
            let item_id = self.item_id;
            Box::pin(async move {
                if matches!(mode, PushPlanMode::Replay) {
                    return Ok(AsyncPushPlan::replay(vec![item_id]));
                }
                let (mut items, ids) = build_push_items(request.items.clone(), 1, 1, 1, 3);
                if request.items.is_empty() {
                    assert!(ids.is_empty());
                } else {
                    assert_eq!(ids, vec![item_id]);
                }
                if matches!(mode, PushPlanMode::SmugglePayload) {
                    items[0].payload = Some(bytes::Bytes::from_static(b"smuggled"));
                }
                let envelope = CommandEnvelope {
                    command_id: CommandId::new("push-command"),
                    request_id: request.request_id.clone(),
                    request_fingerprint: fingerprint.map(|hash| hash.legacy_body_hash.0).map(
                        |hash| {
                            if matches!(mode, PushPlanMode::WrongFingerprint) {
                                hash ^ 1
                            } else {
                                hash
                            }
                        },
                    ),
                    request_outcome: request.request_id.as_ref().map(|_| RequestOutcome::Push {
                        item_ids: ids.clone(),
                    }),
                    item_ids: ids.clone(),
                    command: QueueCommand::Push(PushCommand { items }),
                    checksum: CommandChecksum(0),
                    created_at: request.now,
                };
                Ok(AsyncPushPlan::commit(
                    RawCommitRequest::new(request.shard, vec![envelope], 1),
                    ids,
                ))
            })
        }
    }

    #[derive(Clone)]
    struct ControlledLifecyclePlanner {
        smuggle: LifecycleSmuggle,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum LifecycleSmuggle {
        None,
        Expiry,
        Fault,
        EmptyCommandId,
        Metadata,
        Checksum,
        AppliedState,
        RetryFailedBelowMax,
        RetryPendingAtMax,
    }

    impl AsyncLifecyclePlanner for ControlledLifecyclePlanner {
        fn plan_renew(
            &self,
            request: AsyncRenewRequest,
        ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let smuggle = self.smuggle;
            Box::pin(async move {
                let expires = if smuggle == LifecycleSmuggle::Expiry {
                    UtcTimestamp::new(request.new_lease_expires_at.seconds + 1, 0).unwrap()
                } else {
                    request.new_lease_expires_at
                };
                let item_ids = request
                    .targets
                    .iter()
                    .map(|target| target.item_id)
                    .collect::<Vec<_>>();
                let envelope = CommandEnvelope {
                    command_id: CommandId::new(if smuggle == LifecycleSmuggle::EmptyCommandId {
                        ""
                    } else {
                        "renew-command"
                    }),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids: item_ids.clone(),
                    command: QueueCommand::RenewLease(crate::RenewLeaseCommand {
                        item_ids,
                        lease_expires_at: expires,
                    }),
                    checksum: CommandChecksum(0),
                    created_at: request.now,
                };
                let mut planned = RawCommitRequest::new(request.shard, vec![envelope], 1);
                if smuggle == LifecycleSmuggle::Fault {
                    planned = planned.with_fault(RawCommitFault::BeforeAppend);
                }
                Ok(AsyncLifecyclePlan::renew(planned))
            })
        }

        fn plan_finalize(
            &self,
            request: AsyncFinalizeRequest,
        ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let smuggle = self.smuggle;
            Box::pin(async move {
                let mut expected_outcomes = request
                    .targets
                    .iter()
                    .map(|target| FinalizeOutcome {
                        item_id: target.item_id,
                        kind: target.kind,
                        applied_state: Some(match target.kind {
                            FinalizeKind::Complete => fireweed_core::ItemState::Complete,
                            FinalizeKind::Fail => fireweed_core::ItemState::Failed,
                            FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm => {
                                fireweed_core::ItemState::Pending
                            }
                        }),
                        not_before: target.not_before,
                    })
                    .collect::<Vec<_>>();
                if smuggle == LifecycleSmuggle::RetryPendingAtMax {
                    expected_outcomes[0].applied_state = Some(fireweed_core::ItemState::Failed);
                }
                let mut outcomes = expected_outcomes.clone();
                match smuggle {
                    LifecycleSmuggle::AppliedState => {
                        outcomes[0].applied_state = Some(fireweed_core::ItemState::Leased);
                    }
                    LifecycleSmuggle::RetryFailedBelowMax => {
                        outcomes[0].applied_state = Some(fireweed_core::ItemState::Failed);
                    }
                    LifecycleSmuggle::RetryPendingAtMax => {
                        outcomes[0].applied_state = Some(fireweed_core::ItemState::Pending);
                    }
                    _ => {}
                }
                let item_ids = request
                    .targets
                    .iter()
                    .map(|target| target.item_id)
                    .collect();
                let mut envelope = CommandEnvelope {
                    command_id: CommandId::new(if smuggle == LifecycleSmuggle::EmptyCommandId {
                        ""
                    } else {
                        "finalize-command"
                    }),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids,
                    command: QueueCommand::Finalize(FinalizeCommand { outcomes }),
                    checksum: CommandChecksum(if smuggle == LifecycleSmuggle::Checksum {
                        1
                    } else {
                        0
                    }),
                    created_at: request.now,
                };
                if smuggle == LifecycleSmuggle::Metadata {
                    envelope.request_id = Some(RequestId::new("smuggled").unwrap());
                }
                let mut planned = RawCommitRequest::new(request.shard, vec![envelope], 1);
                if smuggle == LifecycleSmuggle::Fault {
                    planned = planned.with_fault(RawCommitFault::BeforeAppend);
                }
                Ok(AsyncLifecyclePlan::finalize(planned, expected_outcomes))
            })
        }

        fn plan_purge(
            &self,
            request: AsyncPurgeRequest,
        ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let smuggle = self.smuggle;
            Box::pin(async move {
                let mut ids = request.item_ids;
                if smuggle == LifecycleSmuggle::AppliedState {
                    ids.push(ItemId::from_u64(999));
                }
                let mut env = CommandEnvelope {
                    command_id: CommandId::new(if smuggle == LifecycleSmuggle::EmptyCommandId {
                        ""
                    } else {
                        "purge-command"
                    }),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids: ids.clone(),
                    command: QueueCommand::PurgeItems(crate::PurgeItemsCommand {
                        item_ids: ids,
                        force: if smuggle == LifecycleSmuggle::Expiry {
                            !request.force
                        } else {
                            request.force
                        },
                    }),
                    checksum: CommandChecksum(if smuggle == LifecycleSmuggle::Checksum {
                        1
                    } else {
                        0
                    }),
                    created_at: request.now,
                };
                if smuggle == LifecycleSmuggle::Metadata {
                    env.request_id = Some(RequestId::new("smuggled").unwrap());
                }
                let mut raw = RawCommitRequest::new(request.shard, vec![env], 1);
                if smuggle == LifecycleSmuggle::Fault {
                    raw = raw.with_fault(RawCommitFault::BeforeAppend);
                }
                Ok(AsyncLifecyclePlan::purge(raw))
            })
        }
    }

    #[derive(Clone)]
    struct RejectingLeaseResolver {
        error: EngineError,
    }

    impl AsyncLifecyclePlanner for RejectingLeaseResolver {
        fn resolve_lease_targets(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            let error = self.error.clone();
            Box::pin(async move { Err(error) })
        }

        fn plan_renew(
            &self,
            _request: AsyncRenewRequest,
        ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
            panic!("rejected lease targets must not reach lifecycle planning")
        }
    }

    type PushBackend = AsyncComposedBackend<
        UnifiedAtomicCommit<ClaimCommitter>,
        ControlledDispatcher,
        NoAsyncClaimPlanner,
        ControlledPushPlanner,
    >;

    struct PushFixture {
        backend: PushBackend,
        dispatcher: ControlledDispatcher,
        phase: Arc<Phase>,
        commit_calls: Arc<AtomicUsize>,
        commit_completed: Arc<AtomicBool>,
        plan_calls: Arc<AtomicUsize>,
        item_id: ItemId,
    }

    fn push_fixture(mode: PushPlanMode, released: bool) -> PushFixture {
        let phase = Phase::new(released);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let commit_completed = Arc::new(AtomicBool::new(false));
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let item_id = ItemId::mint(1, 1, 1);
        let committer = ClaimCommitter {
            calls: Arc::clone(&commit_calls),
            completed: Arc::clone(&commit_completed),
            phase: Arc::clone(&phase),
        };
        let planner = ControlledPushPlanner {
            mode,
            item_id,
            calls: Arc::clone(&plan_calls),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(4);
        let backend = AsyncComposedBackend::new_with_planners(
            strategy,
            dispatcher.clone(),
            NoAsyncClaimPlanner,
            planner,
            4,
        );
        PushFixture {
            backend,
            dispatcher,
            phase,
            commit_calls,
            commit_completed,
            plan_calls,
            item_id,
        }
    }

    fn planned_operation(
        key: QueueKey,
        phase: Arc<Phase>,
        planned: Arc<Mutex<Vec<QueueKey>>>,
        active_planners: Arc<AtomicUsize>,
        max_active_planners: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
    ) -> impl FnOnce() -> OwnedTask<QueueKey> + Send + 'static {
        move || {
            planned.lock().unwrap().push(key.clone());
            let active = active_planners.fetch_add(1, Ordering::AcqRel) + 1;
            max_active_planners.fetch_max(active, Ordering::AcqRel);
            let mut done = false;
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if phase.released.load(Ordering::Acquire) {
                    if !done {
                        done = true;
                        active_planners.fetch_sub(1, Ordering::AcqRel);
                        finished.fetch_add(1, Ordering::AcqRel);
                    }
                    Poll::Ready(key.clone())
                } else {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }
    }

    #[test]
    fn same_queue_operation_planning_starts_only_after_predecessor_releases() {
        let fixture = fixture(2);
        let first_phase = Phase::new(false);
        let second_phase = Phase::new(true);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut first = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                Arc::clone(&first_phase),
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));
        let mut second = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                second_phase,
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));

        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().as_slice(), &[queue("q")]);
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert!(first_phase.started.load(Ordering::Acquire));
        assert_eq!(planned.lock().unwrap().len(), 1);

        first_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(finished.load(Ordering::Acquire), 1);
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().len(), 2);
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert_eq!(max_active.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn different_queue_operation_planning_progresses_while_first_is_pending() {
        let fixture = fixture(2);
        let stalled_phase = Phase::new(false);
        let ready_phase = Phase::new(true);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut stalled = Box::pin(fixture.backend.submit_operation(
            queue("a"),
            planned_operation(
                queue("a"),
                Arc::clone(&stalled_phase),
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));
        let mut unrelated = Box::pin(fixture.backend.submit_operation(
            queue("b"),
            planned_operation(
                queue("b"),
                ready_phase,
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));

        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().len(), 2);
        assert_eq!(max_active.load(Ordering::Acquire), 2);
        assert!(fixture.dispatcher.drive_next());
        assert!(stalled_phase.started.load(Ordering::Acquire));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(unrelated.as_mut()),
            Poll::Ready(Ok(key)) if key == queue("b")
        ));
        assert_eq!(finished.load(Ordering::Acquire), 1);
        stalled_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn one_backend_dispatches_commit_and_distinct_operation_response_types() {
        let fixture = fixture(2);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("commit"), Phase::new(true));
        let mut commit = Box::pin(fixture.backend.submit_commit(request("commit")));
        let mut rendered = Box::pin(
            fixture
                .backend
                .submit_operation::<String, _>(queue("render"), || {
                    Box::pin(async { "rendered-response".to_string() })
                }),
        );

        assert!(matches!(poll_once(commit.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(rendered.as_mut()), Poll::Pending));
        assert_eq!(fixture.dispatcher.accepted(), 2);
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(commit.as_mut()),
            Poll::Ready(Ok(key)) if key == queue("commit")
        ));
        assert!(matches!(
            poll_once(rendered.as_mut()),
            Poll::Ready(Ok(value)) if value == "rendered-response"
        ));
        assert_eq!(fixture.constructed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_claim_routes_through_strategy_before_rendering() {
        let fixture = claim_fixture(2, true, true);
        let mut claim = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 0);
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Ok(Claimed { items, .. }))
                if items.len() == 1 && items[0].item_id == fixture.item_id
        ));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
        assert!(fixture.commit_completed.load(Ordering::Acquire));
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_push_routes_validated_plan_through_injected_strategy() {
        let fixture = push_fixture(PushPlanMode::Valid, true);
        let mut push = Box::pin(fixture.backend.push(push_request("push", true)));

        assert!(matches!(poll_once(push.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(push.as_mut()),
            Poll::Ready(Ok(outcome)) if outcome.is_fresh() && outcome.item_ids == vec![fixture.item_id]
        ));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
        assert!(fixture.commit_completed.load(Ordering::Acquire));
    }

    #[test]
    fn typed_push_replay_requires_request_id_and_bypasses_commit() {
        let fixture = push_fixture(PushPlanMode::Replay, true);
        let mut replay = Box::pin(fixture.backend.push(push_request("push", true)));
        assert!(matches!(poll_once(replay.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(replay.as_mut()),
            Poll::Ready(Ok(outcome)) if outcome.is_replayed() && outcome.item_ids == vec![fixture.item_id]
        ));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);

        let mut illicit = Box::pin(fixture.backend.push(push_request("push", false)));
        assert!(matches!(poll_once(illicit.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(illicit.as_mut()),
            Poll::Ready(Err(AsyncPushError::BeforeCommit(EngineError::Invalid(
                "invalid async push replay"
            ))))
        ));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_push_rejects_empty_batch_without_request_id_before_planning() {
        let fixture = push_fixture(PushPlanMode::Valid, true);
        let mut request = push_request("push", false);
        request.items.clear();
        let mut push = Box::pin(fixture.backend.push(request));
        assert!(matches!(poll_once(push.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(push.as_mut()),
            Poll::Ready(Err(AsyncPushError::BeforeCommit(EngineError::Invalid(
                "push batch must not be empty"
            ))))
        ));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_push_empty_batch_with_request_id_reaches_planner() {
        // Retained empty enqueues are part of the request-id contract (replay / conflict).
        let fixture = push_fixture(PushPlanMode::Valid, true);
        let mut request = push_request("push", true);
        request.items.clear();
        let mut push = Box::pin(fixture.backend.push(request));
        assert!(matches!(poll_once(push.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        // Planner is consulted (empty request_id batch is no longer rejected pre-plan).
        assert!(fixture.plan_calls.load(Ordering::Acquire) >= 1);
        // Empty plan is valid: one empty Push envelope, zero item ids.
        assert!(matches!(
            poll_once(push.as_mut()),
            Poll::Ready(Ok(outcome)) if outcome.is_fresh() && outcome.item_ids.is_empty()
        ));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_renew_routes_strategy_and_rejects_planner_smuggling() {
        for smuggle in [
            LifecycleSmuggle::None,
            LifecycleSmuggle::Expiry,
            LifecycleSmuggle::Fault,
            LifecycleSmuggle::EmptyCommandId,
        ] {
            let phase = Phase::new(true);
            let commit_calls = Arc::new(AtomicUsize::new(0));
            let strategy = UnifiedAtomicCommit::for_profile(
                DurabilityClass::Atomic,
                ClaimCommitter {
                    calls: commit_calls.clone(),
                    completed: Arc::new(AtomicBool::new(false)),
                    phase,
                },
            )
            .unwrap();
            let dispatcher = ControlledDispatcher::new(4);
            let planner_calls = Arc::new(AtomicUsize::new(0));
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
                .with_lifecycle_planner(ControlledLifecyclePlanner {
                    smuggle,
                    calls: planner_calls.clone(),
                });
            let request = AsyncRenewRequest {
                shard: QueueKey::new(
                    TenantId::new("tenant").unwrap(),
                    QueueId::new("renew").unwrap(),
                ),
                targets: vec![RenewTarget {
                    item_id: ItemId::mint(1, 1, 1),
                    lease_token: LeaseToken::new("renew-token").unwrap(),
                }],
                new_lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
                now: UtcTimestamp::new(10, 0).unwrap(),
                expected_epoch: Some(1),
            };
            let mut renew = Box::pin(backend.renew(request));
            assert!(matches!(poll_once(renew.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            if smuggle != LifecycleSmuggle::None {
                assert!(matches!(
                    poll_once(renew.as_mut()),
                    Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(
                        EngineError::Invalid("invalid async renew plan")
                    )))
                ));
                assert_eq!(commit_calls.load(Ordering::Acquire), 0);
            } else {
                assert!(matches!(poll_once(renew.as_mut()), Poll::Ready(Ok(()))));
                assert_eq!(commit_calls.load(Ordering::Acquire), 1);
            }
            assert_eq!(planner_calls.load(Ordering::Acquire), 1);
        }
    }

    fn renew_request() -> AsyncRenewRequest {
        AsyncRenewRequest {
            shard: QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("renew").unwrap(),
            ),
            targets: vec![RenewTarget {
                item_id: ItemId::mint(1, 1, 1),
                lease_token: LeaseToken::new("renew-token").unwrap(),
            }],
            new_lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
            now: UtcTimestamp::new(10, 0).unwrap(),
            expected_epoch: Some(1),
        }
    }

    fn purge_request(ids: Vec<ItemId>) -> AsyncPurgeRequest {
        AsyncPurgeRequest {
            shard: QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("purge").unwrap(),
            ),
            item_ids: ids,
            force: true,
            now: UtcTimestamp::new(10, 0).unwrap(),
            expected_epoch: Some(1),
        }
    }

    #[test]
    fn purge_plan_accepts_unique_subsets_and_rejects_duplicates_and_foreign_ids() {
        let planner = ControlledLifecyclePlanner {
            smuggle: LifecycleSmuggle::None,
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let validate = |requested: Vec<u64>, planned: Vec<u64>| {
            let request = purge_request(requested.into_iter().map(ItemId::from_u64).collect());
            let mut planned_request = request.clone();
            planned_request.item_ids = planned.into_iter().map(ItemId::from_u64).collect();
            let mut future = planner.plan_purge(planned_request);
            let Poll::Ready(Ok(plan)) = poll_once(future.as_mut()) else {
                panic!("fixture must produce a plan immediately")
            };
            validate_purge_plan(&request, &plan.request)
        };
        // Requests may repeat IDs; a valid planned subset may not.
        assert_eq!(validate(vec![1, 2, 2, 3], vec![3, 1]).unwrap(), 2);
        assert_eq!(validate(vec![1, 2, 2, 3], vec![]).unwrap(), 0);
        assert_eq!(validate(vec![], vec![]).unwrap(), 0);
        assert!(validate(vec![1, 2, 2, 3], vec![2, 2]).is_err());
        assert!(validate(vec![1, 2, 3], vec![1, 4]).is_err());
        assert!(validate(vec![], vec![1]).is_err());
        // Retention-sized batches and arbitrary ordering retain the same contract.
        let large: Vec<_> = (1..=8192).collect();
        assert_eq!(
            validate(large.clone(), large.iter().rev().copied().collect()).unwrap(),
            8192
        );
        let mut late_duplicate = large.clone();
        late_duplicate.push(1);
        assert!(validate(large.clone(), late_duplicate).is_err());
        let mut late_foreign = large.clone();
        late_foreign.push(8193);
        assert!(validate(large, late_foreign).is_err());
    }

    #[test]
    fn typed_purge_routes_counts_skips_empty_and_rejects_smuggling() {
        for (smuggle, commits_expected) in [
            (LifecycleSmuggle::None, 1),
            (LifecycleSmuggle::AppliedState, 0),
            (LifecycleSmuggle::Expiry, 0),
            (LifecycleSmuggle::Metadata, 0),
            (LifecycleSmuggle::Checksum, 0),
            (LifecycleSmuggle::Fault, 0),
            (LifecycleSmuggle::EmptyCommandId, 0),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let dispatcher = ControlledDispatcher::new(2);
            let strategy = UnifiedAtomicCommit::for_profile(
                DurabilityClass::Atomic,
                ClaimCommitter {
                    calls: calls.clone(),
                    completed: Arc::new(AtomicBool::new(false)),
                    phase: Phase::new(true),
                },
            )
            .unwrap();
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
                .with_lifecycle_planner(ControlledLifecyclePlanner {
                    smuggle,
                    calls: Arc::new(AtomicUsize::new(0)),
                });
            let mut future = Box::pin(backend.purge(purge_request(vec![ItemId::from_u64(1)])));
            assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            if commits_expected == 1 {
                assert!(matches!(poll_once(future.as_mut()), Poll::Ready(Ok(1))));
            } else {
                assert!(matches!(
                    poll_once(future.as_mut()),
                    Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(_)))
                ));
            }
            assert_eq!(calls.load(Ordering::Acquire), commits_expected);
        }
        let dispatcher = ControlledDispatcher::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: calls.clone(),
                completed: Arc::new(AtomicBool::new(false)),
                phase: Phase::new(true),
            },
        )
        .unwrap();
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: Arc::new(AtomicUsize::new(0)),
            });
        let mut empty = Box::pin(backend.purge(purge_request(vec![])));
        assert!(matches!(poll_once(empty.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(poll_once(empty.as_mut()), Poll::Ready(Ok(0))));
        assert_eq!(calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_purge_reports_invalid_response_barrier() {
        let dispatcher = ControlledDispatcher::new(1);
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, AppendedOnlyCommitter)
                .unwrap();
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: Arc::new(AtomicUsize::new(0)),
            });
        let mut future = Box::pin(backend.purge(purge_request(vec![ItemId::from_u64(1)])));
        assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(future.as_mut()),
            Poll::Ready(Err(AsyncLifecycleError::AfterCommit {
                stage: AsyncLifecyclePostCommitStage::CommitOutcome,
                source: EngineError::Storage(_)
            }))
        ));
    }

    #[test]
    fn dropping_typed_purge_response_does_not_cancel_accepted_commit() {
        let phase = Phase::new(false);
        let completed = Arc::new(AtomicBool::new(false));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: Arc::new(AtomicUsize::new(0)),
                completed: completed.clone(),
                phase: phase.clone(),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: Arc::new(AtomicUsize::new(0)),
            });
        let mut purge = Box::pin(backend.purge(purge_request(vec![ItemId::from_u64(1)])));
        assert!(matches!(poll_once(purge.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(purge);
        phase.release();
        assert!(dispatcher.drive_next());
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn typed_purge_serializes_same_queue_across_planning_and_commit() {
        let phase = Phase::new(false);
        let planner_calls = Arc::new(AtomicUsize::new(0));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: Arc::new(AtomicUsize::new(0)),
                completed: Arc::new(AtomicBool::new(false)),
                phase: phase.clone(),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(2);
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: planner_calls.clone(),
            });
        let mut first = Box::pin(backend.purge(purge_request(vec![ItemId::from_u64(1)])));
        let mut second = Box::pin(backend.purge(purge_request(vec![ItemId::from_u64(2)])));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert_eq!(planner_calls.load(Ordering::Acquire), 1);
        phase.release();
        assert!(dispatcher.drive_next());
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(1))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(1))));
        assert_eq!(planner_calls.load(Ordering::Acquire), 2);
    }

    fn finalize_request() -> AsyncFinalizeRequest {
        AsyncFinalizeRequest {
            shard: queue("finalize"),
            targets: vec![FinalizeTarget {
                item_id: ItemId::mint(1, 1, 1),
                lease_token: LeaseToken::new("finalize-token").unwrap(),
                item_version: 2,
                kind: FinalizeKind::Complete,
                not_before: None,
            }],
            now: UtcTimestamp::new(10, 0).unwrap(),
            expected_epoch: Some(1),
        }
    }

    struct FinalizeFixture {
        backend: AsyncComposedBackend<
            UnifiedAtomicCommit<ClaimCommitter>,
            ControlledDispatcher,
            NoAsyncClaimPlanner,
            NoAsyncPushPlanner,
            ControlledLifecyclePlanner,
        >,
        dispatcher: ControlledDispatcher,
        phase: Arc<Phase>,
        commits: Arc<AtomicUsize>,
        completed: Arc<AtomicBool>,
        plans: Arc<AtomicUsize>,
    }

    fn finalize_backend(
        smuggle: LifecycleSmuggle,
        released: bool,
        capacity: usize,
    ) -> FinalizeFixture {
        let phase = Phase::new(released);
        let commits = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let plans = Arc::new(AtomicUsize::new(0));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: commits.clone(),
                completed: completed.clone(),
                phase: phase.clone(),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), capacity)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle,
                calls: plans.clone(),
            });
        FinalizeFixture {
            backend,
            dispatcher,
            phase,
            commits,
            completed,
            plans,
        }
    }

    #[test]
    fn item_id_finalize_preserves_lease_rejection_precedence_before_commit() {
        for expected in [
            EngineError::NotFound,
            EngineError::StaleLease,
            EngineError::Terminal,
            EngineError::Superseded,
            EngineError::Invalid("item is not leased"),
        ] {
            let commits = Arc::new(AtomicUsize::new(0));
            let strategy = UnifiedAtomicCommit::for_profile(
                DurabilityClass::Atomic,
                ClaimCommitter {
                    calls: Arc::clone(&commits),
                    completed: Arc::new(AtomicBool::new(false)),
                    phase: Phase::new(true),
                },
            )
            .unwrap();
            let dispatcher = ControlledDispatcher::new(1);
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
                .with_lifecycle_planner(RejectingLeaseResolver {
                    error: expected.clone(),
                });
            let shard = queue("lease-rejection");
            let mut finalize = Box::pin(backend.finalize_outcomes(
                shard,
                vec![FinalizeOutcome::new(
                    ItemId::from_u64(1),
                    FinalizeKind::Complete,
                )],
                UtcTimestamp::new(10, 0).unwrap(),
                None,
            ));

            assert!(matches!(poll_once(finalize.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            assert_eq!(
                poll_once(finalize.as_mut()),
                Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(expected)))
            );
            assert_eq!(commits.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn typed_finalize_routes_strategy_and_rejects_smuggling_before_commit() {
        for smuggle in [
            LifecycleSmuggle::None,
            LifecycleSmuggle::Fault,
            LifecycleSmuggle::EmptyCommandId,
            LifecycleSmuggle::Metadata,
            LifecycleSmuggle::Checksum,
            LifecycleSmuggle::AppliedState,
        ] {
            let fixture = finalize_backend(smuggle, true, 1);
            let mut finalize = Box::pin(fixture.backend.finalize(finalize_request()));
            assert!(matches!(poll_once(finalize.as_mut()), Poll::Pending));
            assert!(fixture.dispatcher.drive_next());
            if smuggle == LifecycleSmuggle::None {
                assert!(matches!(poll_once(finalize.as_mut()), Poll::Ready(Ok(()))));
                assert_eq!(fixture.commits.load(Ordering::Acquire), 1);
            } else {
                assert!(matches!(
                    poll_once(finalize.as_mut()),
                    Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(
                        EngineError::Invalid("invalid async finalize plan")
                    )))
                ));
                assert_eq!(fixture.commits.load(Ordering::Acquire), 0);
            }
            assert_eq!(fixture.plans.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn typed_finalize_rejects_empty_duplicate_and_stale_epoch_before_commit() {
        for mutate in 0..3 {
            let fixture = finalize_backend(LifecycleSmuggle::None, true, 1);
            let mut request = finalize_request();
            match mutate {
                0 => request.targets.clear(),
                1 => request.targets.push(request.targets[0].clone()),
                2 => request.expected_epoch = Some(2),
                _ => unreachable!(),
            }
            let mut finalize = Box::pin(fixture.backend.finalize(request));
            assert!(matches!(poll_once(finalize.as_mut()), Poll::Pending));
            assert!(fixture.dispatcher.drive_next());
            assert!(matches!(
                poll_once(finalize.as_mut()),
                Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(_)))
            ));
            assert_eq!(fixture.commits.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn typed_finalize_rejects_retry_state_opposite_to_authoritative_attempt_count() {
        for smuggle in [
            LifecycleSmuggle::RetryFailedBelowMax,
            LifecycleSmuggle::RetryPendingAtMax,
        ] {
            let fixture = finalize_backend(smuggle, true, 1);
            let mut request = finalize_request();
            request.targets[0].kind = FinalizeKind::Retry;
            request.targets[0].not_before = Some(UtcTimestamp::new(20, 0).unwrap());
            let mut finalize = Box::pin(fixture.backend.finalize(request));
            assert!(matches!(poll_once(finalize.as_mut()), Poll::Pending));
            assert!(fixture.dispatcher.drive_next());
            assert!(matches!(
                poll_once(finalize.as_mut()),
                Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(
                    EngineError::Invalid("invalid async finalize plan")
                )))
            ));
            assert_eq!(fixture.commits.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn typed_finalize_reports_invalid_response_barrier_after_commit() {
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new(
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, AppendedOnlyCommitter)
                .unwrap(),
            dispatcher.clone(),
            1,
        )
        .with_lifecycle_planner(ControlledLifecyclePlanner {
            smuggle: LifecycleSmuggle::None,
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let mut finalize = Box::pin(backend.finalize(finalize_request()));
        assert!(matches!(poll_once(finalize.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(finalize.as_mut()),
            Poll::Ready(Err(AsyncLifecycleError::AfterCommit {
                stage: AsyncLifecyclePostCommitStage::CommitOutcome,
                ..
            }))
        ));
    }

    #[test]
    fn typed_finalize_serializes_same_queue_and_survives_dropped_caller() {
        let fixture = finalize_backend(LifecycleSmuggle::None, false, 2);
        let mut first = Box::pin(fixture.backend.finalize(finalize_request()));
        let mut second = Box::pin(fixture.backend.finalize(finalize_request()));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(fixture.plans.load(Ordering::Acquire), 1);
        drop(first);
        fixture.phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.completed.load(Ordering::Acquire));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(()))));
        assert_eq!(fixture.plans.load(Ordering::Acquire), 2);
        assert_eq!(fixture.commits.load(Ordering::Acquire), 2);
    }

    #[test]
    fn typed_renew_reports_invalid_response_barrier_after_commit() {
        let dispatcher = ControlledDispatcher::new(1);
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, AppendedOnlyCommitter)
                .unwrap();
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: Arc::new(AtomicUsize::new(0)),
            });
        let mut renew = Box::pin(backend.renew(renew_request()));
        assert!(matches!(poll_once(renew.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(renew.as_mut()),
            Poll::Ready(Err(AsyncLifecycleError::AfterCommit {
                stage: AsyncLifecyclePostCommitStage::CommitOutcome,
                source: EngineError::Storage(_),
            }))
        ));
    }

    #[test]
    fn dropping_typed_renew_response_does_not_cancel_accepted_commit() {
        let phase = Phase::new(false);
        let completed = Arc::new(AtomicBool::new(false));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: Arc::new(AtomicUsize::new(0)),
                completed: completed.clone(),
                phase: phase.clone(),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: Arc::new(AtomicUsize::new(0)),
            });
        let mut renew = Box::pin(backend.renew(renew_request()));
        assert!(matches!(poll_once(renew.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(renew);
        phase.release();
        assert!(dispatcher.drive_next());
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn typed_renew_serializes_same_queue_across_planning_and_commit() {
        let phase = Phase::new(false);
        let planner_calls = Arc::new(AtomicUsize::new(0));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: Arc::new(AtomicUsize::new(0)),
                completed: Arc::new(AtomicBool::new(false)),
                phase: phase.clone(),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(2);
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: planner_calls.clone(),
            });
        let mut first = Box::pin(backend.renew(renew_request()));
        let mut second = Box::pin(backend.renew(renew_request()));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert_eq!(planner_calls.load(Ordering::Acquire), 1);
        phase.release();
        assert!(dispatcher.drive_next());
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(()))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(()))));
        assert_eq!(planner_calls.load(Ordering::Acquire), 2);
    }

    #[test]
    fn static_push_shape_enforces_priority_gates_cohorts_and_groups() {
        let mut def = definition("push");
        def.max_eligible_group_size = Some(10);
        let mut spec = PushSpec {
            priority: Some(PriorityValue::Text("wrong".into())),
            ..Default::default()
        };
        assert!(validate_push_shape(&def, &[spec.clone()]).is_err());

        spec.priority = None;
        spec.gate_keys = vec!["same".into(), "same".into()];
        assert!(validate_push_shape(&def, &[spec.clone()]).is_err());

        spec.gate_keys.clear();
        spec.group_key = Some(fireweed_core::GroupKey::new("cohort").unwrap());
        spec.cohort_size = Some(2);
        assert!(validate_push_shape(&def, &[spec.clone()]).is_err());

        def.cohort_policy = Some(fireweed_core::CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(60_000),
            on_incomplete: None,
            max_cohort_size: Some(1),
        });
        assert!(validate_push_shape(&def, &[spec.clone()]).is_err());

        spec.cohort_size = None;
        def.max_eligible_group_size = None;
        assert!(validate_push_shape(&def, &[spec]).is_err());

        let mut ordinary = definition("ordinary");
        ordinary.max_eligible_group_size = None;
        let grouped = PushSpec {
            group_key: Some(fireweed_core::GroupKey::new("ordinary-group").unwrap()),
            ..Default::default()
        };
        assert!(validate_push_shape(&ordinary, &[grouped]).is_ok());
    }

    #[test]
    fn duplicate_gate_keys_canonicalize_before_fingerprinting() {
        let mut duplicated = vec![PushSpec {
            gate_keys: vec!["z".into(), "a".into(), "z".into()],
            ..Default::default()
        }];
        let mut canonical = vec![PushSpec {
            gate_keys: vec!["a".into(), "z".into()],
            ..Default::default()
        }];
        canonicalize_push_gate_keys(&mut duplicated);
        canonicalize_push_gate_keys(&mut canonical);
        assert_eq!(duplicated[0].gate_keys, canonical[0].gate_keys);
        assert_eq!(
            crate::push_specs_fingerprint_sha256(&duplicated).unwrap(),
            crate::push_specs_fingerprint_sha256(&canonical).unwrap()
        );
    }

    #[test]
    fn push_item_matches_explicit_index_fields_without_entity() {
        let def = definition("typed");
        let spec = PushSpec {
            index_fields: BTreeMap::from([(
                "s".into(),
                fireweed_core::TypedValue::String("native".into()),
            )]),
            ..Default::default()
        };
        let (mut items, ids) =
            build_push_items(vec![spec.clone()], 1, 1, 1, def.retry_policy.max_attempts);
        crate::admit_push_items_indexes(&def, &mut items).unwrap();
        assert!(push_item_matches(&items[0], &spec, ids[0], &def));

        let mut tampered = items[0].clone();
        tampered.index_fields.insert(
            "s".into(),
            fireweed_core::TypedValue::String("wrong".into()),
        );
        assert!(!push_item_matches(&tampered, &spec, ids[0], &def));
    }

    #[test]
    fn push_item_matches_entity_derived_index_fields() {
        use axon_esf::{IndexDeclaration, IndexDef};
        use fireweed_core::{IndexType, QueueIndex};

        let mut def = definition("typed-entity");
        def.typed_indexes = vec![QueueIndex {
            name: "by_s".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "s".into(),
                index_type: IndexType::String,
                unique: false,
            }),
        }];
        let spec = PushSpec {
            entity: Some(serde_json::json!({"s": "from-entity"})),
            ..Default::default()
        };
        let (mut items, ids) =
            build_push_items(vec![spec.clone()], 1, 1, 1, def.retry_policy.max_attempts);
        crate::admit_push_items_indexes(&def, &mut items).unwrap();
        assert!(push_item_matches(&items[0], &spec, ids[0], &def));
    }

    #[test]
    fn typed_push_rejects_planner_smuggling_before_commit() {
        for mode in [PushPlanMode::SmugglePayload, PushPlanMode::WrongFingerprint] {
            let fixture = push_fixture(mode, true);
            let mut push = Box::pin(fixture.backend.push(push_request("push", true)));
            assert!(matches!(poll_once(push.as_mut()), Poll::Pending));
            assert!(fixture.dispatcher.drive_next());
            assert!(matches!(
                poll_once(push.as_mut()),
                Poll::Ready(Err(AsyncPushError::BeforeCommit(EngineError::Invalid(
                    "invalid async push plan"
                ))))
            ));
            assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn dropping_typed_push_response_does_not_cancel_accepted_commit() {
        let fixture = push_fixture(PushPlanMode::Valid, false);
        let mut push = Box::pin(fixture.backend.push(push_request("push", true)));

        assert!(matches!(poll_once(push.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.phase.started.load(Ordering::Acquire));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
        drop(push);

        fixture.phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.commit_completed.load(Ordering::Acquire));
    }

    struct AppendedOnlyCommitter;

    impl UnifiedAtomicCommitter for AppendedOnlyCommitter {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            let position =
                CommandPosition::new(request.shard().clone(), request.expected_epoch(), 1);
            Box::pin(async move { Ok(RawCommitOutcome::appended(vec![position])) })
        }
    }

    #[test]
    fn typed_push_reports_invalid_response_barrier_after_commit() {
        let dispatcher = ControlledDispatcher::new(1);
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, AppendedOnlyCommitter)
                .unwrap();
        let backend = AsyncComposedBackend::new_with_planners(
            strategy,
            dispatcher.clone(),
            NoAsyncClaimPlanner,
            ControlledPushPlanner {
                mode: PushPlanMode::Valid,
                item_id: ItemId::mint(1, 1, 1),
                calls: Arc::new(AtomicUsize::new(0)),
            },
            1,
        );
        let mut push = Box::pin(backend.push(push_request("push", true)));

        assert!(matches!(poll_once(push.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(push.as_mut()),
            Poll::Ready(Err(AsyncPushError::AfterCommit {
                stage: AsyncPushPostCommitStage::CommitOutcome,
                source: EngineError::Storage(_),
            }))
        ));
    }

    #[test]
    fn typed_claim_keeps_queue_permit_across_plan_commit_and_render() {
        let fixture = claim_fixture(2, false, false);
        let mut first = Box::pin(fixture.backend.claim(claim_request("claim")));
        let mut second = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.phase.started.load(Ordering::Acquire));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);

        fixture.phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.render_phase.started.load(Ordering::Acquire));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);

        fixture.render_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 2);
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 2);
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 2);
    }

    #[test]
    fn cancelling_typed_claim_caller_does_not_cancel_accepted_commit() {
        let fixture = claim_fixture(1, false, true);
        let mut claim = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.phase.started.load(Ordering::Acquire));
        drop(claim);
        fixture.phase.release();
        assert!(fixture.dispatcher.drive_next());

        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
        assert!(fixture.commit_completed.load(Ordering::Acquire));
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 1);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn one_claim_backend_dispatches_raw_commit_and_claim_response_types() {
        let fixture = claim_fixture(2, true, true);
        let mut raw = Box::pin(fixture.backend.submit_commit(request("raw")));
        let mut claim = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(raw.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(raw.as_mut()), Poll::Ready(Ok(Ok(_)))));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Ready(Ok(_))));
    }

    #[derive(Clone)]
    struct InvalidClaimPlanner {
        item_id: ItemId,
    }

    impl AsyncClaimPlanner for InvalidClaimPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            let item_id = self.item_id;
            Box::pin(async move {
                let mut envelope = claim_envelope(&request, item_id);
                envelope.command = QueueCommand::ResumeQueue;
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(request.shard, vec![envelope], 1),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            panic!("an invalid plan must not render")
        }
    }

    #[test]
    fn typed_claim_rejects_non_claim_mutation_before_strategy() {
        let phase = Phase::new(true);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = ClaimCommitter {
            calls: Arc::clone(&commit_calls),
            completed: Arc::new(AtomicBool::new(false)),
            phase,
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            InvalidClaimPlanner {
                item_id: ItemId::mint(1, 1, 1),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::BeforeCommit(EngineError::Invalid(
                "invalid async claim plan"
            ))))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn claim_plan_rejects_empty_duplicate_oversized_and_mismatched_cohort_candidates() {
        let request = claim_request("claim");
        let first = ItemId::mint(1, 1, 1);
        let second = ItemId::mint(1, 1, 2);
        let valid = |ids: Vec<ItemId>| {
            RawCommitRequest::new(
                request.shard.clone(),
                vec![claim_envelope(&request, ids[0])],
                1,
            )
        };

        let empty_commit = RawCommitRequest::new(request.shard.clone(), Vec::new(), 1);
        assert!(validate_claim_plan(&request, &request.shard, &empty_commit, &[], None).is_err());

        let mut duplicate_envelope = claim_envelope(&request, first);
        duplicate_envelope.item_ids = vec![first, first];
        if let QueueCommand::Claim(command) = &mut duplicate_envelope.command {
            command.item_ids = vec![first, first];
        }
        let duplicate_commit =
            RawCommitRequest::new(request.shard.clone(), vec![duplicate_envelope], 1);
        assert!(
            validate_claim_plan(
                &request,
                &request.shard,
                &duplicate_commit,
                &[first, first],
                None
            )
            .is_err()
        );

        let mut oversized_request = request.clone();
        oversized_request.max_items = 1;
        let mut oversized_envelope = claim_envelope(&oversized_request, first);
        oversized_envelope.item_ids = vec![first, second];
        if let QueueCommand::Claim(command) = &mut oversized_envelope.command {
            command.item_ids = vec![first, second];
        }
        let oversized_commit =
            RawCommitRequest::new(oversized_request.shard.clone(), vec![oversized_envelope], 1);
        assert!(
            validate_claim_plan(
                &oversized_request,
                &oversized_request.shard,
                &oversized_commit,
                &[first, second],
                None
            )
            .is_err()
        );

        let ordinary_commit = valid(vec![first]);
        assert!(
            validate_claim_plan(
                &request,
                &request.shard,
                &ordinary_commit,
                &[first],
                Some(&CohortId::new("cohort").unwrap())
            )
            .is_err()
        );

        let mut cohort_request = request.clone();
        cohort_request.compatibility.whole_cohort = true;
        let cohort_shape_mismatch = RawCommitRequest::new(
            cohort_request.shard.clone(),
            vec![claim_envelope(&cohort_request, first)],
            1,
        );
        assert!(
            validate_claim_plan(
                &cohort_request,
                &cohort_request.shard,
                &cohort_shape_mismatch,
                &[first],
                None
            )
            .is_err()
        );
    }

    #[test]
    fn claim_plan_rejects_smuggled_envelope_metadata_time_and_checksum() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let mut envelope = claim_envelope(&request, item_id);
        envelope.request_fingerprint = Some(7);
        let commit = RawCommitRequest::new(request.shard.clone(), vec![envelope], 1);
        assert!(validate_claim_plan(&request, &request.shard, &commit, &[item_id], None).is_err());

        let mut envelope = claim_envelope(&request, item_id);
        envelope.created_at = UtcTimestamp::new(11, 0).unwrap();
        let commit = RawCommitRequest::new(request.shard.clone(), vec![envelope], 1);
        assert!(validate_claim_plan(&request, &request.shard, &commit, &[item_id], None).is_err());

        let mut envelope = claim_envelope(&request, item_id);
        envelope.checksum = CommandChecksum(9);
        let commit = RawCommitRequest::new(request.shard.clone(), vec![envelope], 1);
        assert!(validate_claim_plan(&request, &request.shard, &commit, &[item_id], None).is_err());
    }

    #[test]
    fn claim_commit_outcome_and_render_must_match_the_committed_footprint() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let position = CommandPosition::new(request.shard.clone(), 1, 1);
        assert!(
            validate_claim_commit_outcome(
                &request.shard,
                1,
                &RawCommitOutcome::appended(vec![position.clone()])
            )
            .is_err()
        );
        assert!(
            validate_claim_commit_outcome(
                &request.shard,
                2,
                &RawCommitOutcome::applied(vec![position])
            )
            .is_err()
        );

        let mut wrong = claimed_item(&request, item_id);
        wrong.lease_token = Some(LeaseToken::new("wrong").unwrap());
        assert!(validate_rendered_claim(&request, &[item_id], &[wrong]).is_err());
        assert!(validate_rendered_claim(&request, &[item_id], &[]).is_err());
    }

    fn full_row_claimed_item(request: &ClaimRequest, item_id: ItemId, seq: u8) -> ClaimedItem {
        let mut fields = BTreeMap::new();
        fields.insert("blob".to_string(), bytes::Bytes::from(vec![seq, 2, 3]));
        ClaimedItem {
            item_id,
            client_item_key: ClientItemKey::new(format!("key-{item_id}")).unwrap(),
            item_version: 2,
            priority: Some(PriorityValue::Int64(7)),
            group_key: Some(GroupKey::new("group-a").unwrap()),
            not_before: Some(UtcTimestamp::new(7, 0).unwrap()),
            lease_token: Some(request.lease_token.clone()),
            lease_expires_at: request.lease_expires_at,
            attempt_count: 3,
            max_attempts: 9,
            payload: Some(bytes::Bytes::from_static(b"\xCA\xFE")),
            fields,
            metadata: Metadata::from_entries(BTreeMap::from([
                (
                    "source".to_string(),
                    MetadataValue::String("grouped".to_string()),
                ),
                ("attempt".to_string(), MetadataValue::Integer(7)),
            ])),
            gate_keys: vec!["gate-satisfied".to_string()],
            entity: Some(serde_json::json!({"kind": "stored", "rank": 7})),
        }
    }

    #[test]
    fn grouped_cohort_claim_materializes_before_append() {
        let grouped = claim_request("grouped");
        let first = ItemId::mint(1, 1, 1);
        let second = ItemId::mint(1, 1, 2);
        let grouped_items = vec![
            full_row_claimed_item(&grouped, first, 1),
            full_row_claimed_item(&grouped, second, 2),
        ];
        validate_rendered_claim(&grouped, &[first, second], &grouped_items).expect("pre-shape");
        let retained = PreparedClaimedResult::from_rendered(
            &grouped,
            &[first, second],
            grouped_items.clone(),
            None,
        )
        .expect("grouped retained");
        let claimed = retained.into_claimed();
        assert_eq!(claimed.items.len(), 2);
        assert_eq!(claimed.items[0].item_id, first);
        assert_eq!(claimed.items[1].item_id, second);
        assert_eq!(claimed.items[0].payload, grouped_items[0].payload);
        assert_eq!(claimed.items[0].fields, grouped_items[0].fields);
        assert_eq!(claimed.items[0].metadata, grouped_items[0].metadata);
        assert_eq!(claimed.items[0].entity, grouped_items[0].entity);
        assert_eq!(claimed.items[0].gate_keys, grouped_items[0].gate_keys);
        assert_eq!(claimed.items[0].not_before, grouped_items[0].not_before);
        assert_eq!(
            claimed.items[0].lease_token.as_ref(),
            Some(&grouped.lease_token)
        );
        assert_eq!(claimed.items[0].lease_expires_at, grouped.lease_expires_at);
        assert!(claimed.cohort_lease_token.is_none());
        assert!(claimed.cohort_id.is_none());

        let mut cohort_request = grouped.clone();
        cohort_request.compatibility.whole_cohort = true;
        let cohort_id = CohortId::new("coh:group-a:10000000000").unwrap();
        let shaped = finish_retained_grouped_cohort_claim(
            &cohort_request,
            &[first, second],
            grouped_items,
            Some(cohort_id.clone()),
        )
        .expect("cohort shape");
        assert_eq!(shaped.items.len(), 2);
        assert_eq!(shaped.items[0].item_id, first);
        assert_eq!(shaped.items[1].item_id, second);
        assert!(shaped.items.iter().all(|item| item.lease_token.is_none()));
        assert_eq!(
            shaped.cohort_lease_token.as_ref(),
            Some(&cohort_request.lease_token)
        );
        assert_eq!(shaped.cohort_id.as_ref(), Some(&cohort_id));
        let continued = PreparedClaimedResult::Retained(shaped).into_claimed();
        assert_eq!(continued.cohort_id.as_ref(), Some(&cohort_id));

        let source = include_str!("async_composed.rs");
        let retained_helper = source
            .split("pub fn finish_retained_grouped_cohort_claim(")
            .nth(1)
            .expect("retained helper")
            .split("enum ClaimExecutionError")
            .next()
            .expect("retained helper end");
        for needle in [
            "render_claimed",
            "AsyncClaimPlanner",
            "AsyncProjectionStore",
            "planner",
        ] {
            assert!(
                !retained_helper.contains(needle),
                "post-apply continuation must need no projection handle ({needle})"
            );
        }
        let serving = source
            .split("pub async fn claim(")
            .nth(1)
            .expect("serving claim")
            .split("pub async fn prepare_claim(")
            .next()
            .expect("serving claim end");
        assert!(
            serving.contains("finish_rendered_claim(planner, request, item_ids, cohort_id)"),
            "atomic/sqlite serving keeps post-append render; S5 exclusive is plan_exclusive_claim"
        );
        assert!(!serving.contains("PreparedClaimedResult"));
        let exclusive = source
            .split("pub async fn plan_exclusive_claim(")
            .nth(1)
            .expect("exclusive claim")
            .split("pub async fn render_prepared_claim(")
            .next()
            .expect("exclusive claim end");
        assert!(exclusive.contains("with_item_authority_first"));
        assert!(!exclusive.contains("submit_operation"));
        assert!(!exclusive.contains("render_claimed"));
        assert!(!exclusive.contains("SelectionFence"));
        let stamped = PreparedClaim::Commit {
            request: RawCommitRequest::new(
                grouped.shard.clone(),
                vec![claim_envelope(&grouped, first)],
                1,
            ),
            item_ids: vec![first],
            cohort_id: None,
        }
        .with_item_authority_first();
        let PreparedClaim::Commit { request, .. } = stamped else {
            panic!("stamped exclusive item Claim");
        };
        match &request.commands()[0].command {
            QueueCommand::Claim(command) => assert!(command.authority_first),
            other => panic!("expected item Claim, got {other:?}"),
        }
    }

    fn generation_definition() -> QueueDefinition {
        definition("queue")
    }

    fn push_work(index: u8, key: &str) -> MutationGenerationWork {
        let shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        );
        let spec = PushSpec {
            client_item_key: Some(ClientItemKey::new(key).unwrap()),
            payload: Some(bytes::Bytes::from(vec![index])),
            ..PushSpec::default()
        };
        let request = AsyncPushRequest {
            shard: shard.clone(),
            request_id: Some(RequestId::new(format!("req-{index}")).unwrap()),
            items: vec![spec.clone()],
            now: UtcTimestamp::new(1, 0).unwrap(),
            expected_epoch: Some(1),
        };
        MutationGenerationWork::Push {
            fingerprint: Some(PushFingerprint {
                canonical_sha256: [index; 32],
                legacy_body_hash: BodyHash(index as u64),
            }),
            request,
        }
    }

    fn empty_snapshot() -> MutationDriverSnapshot {
        MutationDriverSnapshot {
            definition: generation_definition(),
            paused_drain_intake: false,
            client_keys: HashSet::new(),
            request_fingerprints: HashMap::new(),
            unique_index_values: HashSet::new(),
            group_counts: HashMap::new(),
            batch_items: Vec::new(),
            leased_ids: HashSet::new(),
            terminal_ids: HashSet::new(),
        }
    }

    #[test]
    fn inert_mutation_generation_validates_fifo_overlay_and_releases_slot() {
        use std::time::Duration;

        use crate::{
            BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateOutcome, BatchUpdateValue,
            CLAIM_GENERATION_MAX_REQUESTS, MUTATION_MAX_REQUESTS_PER_QUEUE,
            MUTATION_SEQUENCER_RESOURCE, MutationIngress, MutationSequencer,
            SharedDriverReadAdmission,
        };

        let snapshot = empty_snapshot();
        let works: Vec<MutationGenerationWork> = (0..CLAIM_GENERATION_MAX_REQUESTS as u8)
            .map(|index| push_work(index, &format!("key-{index}")))
            .collect();
        let members = validate_inert_mutation_generation(&snapshot, &works).expect("overlay");
        assert_eq!(members.len(), CLAIM_GENERATION_MAX_REQUESTS);
        assert!(members.iter().all(|member| {
            matches!(
                member.outcome,
                MutationGenerationMemberOutcome::PushAccepted
            )
        }));

        let mut conflict_works = vec![
            push_work(1, "shared"),
            push_work(2, "shared"),
            push_work(3, "other"),
        ];
        if let MutationGenerationWork::Push { request, .. } = &mut conflict_works[0] {
            request.items[0].group_key = Some(GroupKey::new("g").unwrap());
        }
        if let MutationGenerationWork::Push { request, .. } = &mut conflict_works[2] {
            request.items[0].group_key = Some(GroupKey::new("g").unwrap());
        }
        let overlay = validate_inert_mutation_generation(&snapshot, &conflict_works).unwrap();
        assert!(matches!(
            overlay[0].outcome,
            MutationGenerationMemberOutcome::PushAccepted
        ));
        assert!(matches!(
            overlay[1].outcome,
            MutationGenerationMemberOutcome::Rejected(EngineError::Conflict)
        ));
        assert!(matches!(
            overlay[2].outcome,
            MutationGenerationMemberOutcome::PushAccepted
        ));

        let sequencer =
            MutationSequencer::<QueueKey, MutationSequencerKey, MutationGenerationWork>::new();
        let queue = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        );
        let mut tickets = Vec::new();
        for index in 0..MUTATION_MAX_REQUESTS_PER_QUEUE as u8 {
            tickets.push(
                sequencer
                    .admit(
                        queue.clone(),
                        MutationSequencerKey::Compatible(MutationGenerationKind::Push),
                        if index % 2 == 0 {
                            MutationIngress::Direct
                        } else {
                            MutationIngress::KeyedPermitLive
                        },
                        Arc::new(push_work(index, &format!("k-{index}"))),
                        1,
                        1,
                    )
                    .expect("two generations of eight"),
            );
        }
        assert_eq!(sequencer.generation_count(&queue), 2);
        assert!(matches!(
            sequencer.admit(
                queue.clone(),
                MutationSequencerKey::Compatible(MutationGenerationKind::Push),
                MutationIngress::Direct,
                Arc::new(push_work(99, "overflow")),
                1,
                1,
            ),
            Err(crate::CoordinationError::Capacity {
                resource: MUTATION_SEQUENCER_RESOURCE,
            })
        ));
        let first = sequencer
            .start_generation(&queue)
            .expect("active generation");
        assert_eq!(first.requests().len(), CLAIM_GENERATION_MAX_REQUESTS);
        assert!(sequencer.start_generation(&queue).is_none());
        first.complete();
        let suffix = sequencer.start_generation(&queue).expect("suffix re-drive");
        assert_eq!(suffix.requests().len(), CLAIM_GENERATION_MAX_REQUESTS);
        suffix.complete();

        let singleton_seq =
            MutationSequencer::<QueueKey, MutationSequencerKey, MutationGenerationWork>::new();
        let keyed = Arc::new(MutationGenerationWork::Singleton {
            id: 7,
            commit: RawCommitRequest::new(queue.clone(), Vec::new(), 1),
        });
        let complex = Arc::new(MutationGenerationWork::Singleton {
            id: 8,
            commit: RawCommitRequest::new(queue.clone(), Vec::new(), 1),
        });
        let first_ticket = singleton_seq
            .admit(
                queue.clone(),
                keyed.sequencer_key(),
                MutationIngress::KeyedPermitLive,
                keyed,
                1,
                1,
            )
            .unwrap();
        let second_ticket = singleton_seq
            .admit(
                queue.clone(),
                complex.sequencer_key(),
                MutationIngress::Direct,
                complex,
                1,
                1,
            )
            .unwrap();
        assert_eq!(singleton_seq.generation_count(&queue), 2);
        assert_ne!(first_ticket.generation_id(), second_ticket.generation_id());

        let batch = MutationGenerationWork::BatchUpdate {
            shard: queue.clone(),
            request: BatchUpdateRequest {
                request_id: RequestId::new("batch-1").unwrap(),
                updates: vec![BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ItemId(ItemId::mint(1, 1, 1)),
                    expected_item_version: Some(1),
                    priority: BatchUpdateValue::Keep,
                    not_before: BatchUpdateValue::Keep,
                    payload: BatchUpdateValue::Keep,
                    metadata: BatchUpdateValue::Keep,
                    gate_keys: BatchUpdateValue::Keep,
                    fields: BatchUpdateValue::Keep,
                }],
            },
            now: UtcTimestamp::new(1, 0).unwrap(),
            expected_epoch: 1,
            fingerprint: BodyHash(1),
            command_id: CommandId::new("batch-1"),
        };
        let mut batch_snapshot = empty_snapshot();
        batch_snapshot.batch_items.push(BatchUpdateSnapshotItem {
            item_id: ItemId::mint(1, 1, 1),
            client_item_key: ClientItemKey::new("live").unwrap(),
            state: ItemState::Pending,
            item_version: 1,
            fenced: false,
            superseded: false,
        });
        let batch_members =
            validate_inert_mutation_generation(&batch_snapshot, std::slice::from_ref(&batch))
                .unwrap();
        match &batch_members[0].outcome {
            MutationGenerationMemberOutcome::BatchUpdate { response, .. } => {
                assert!(matches!(
                    response.results[0],
                    BatchUpdateOutcome::Updated {
                        item_version: 2,
                        ..
                    }
                ));
            }
            other => panic!("expected batch overlay, got {other:?}"),
        }

        let slots = SharedDriverReadAdmission::default();
        let mut acquire = slots.acquire();
        let permit = match poll_once(Pin::new(&mut acquire)) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("shared driver slot"),
        };
        let planned = sequencer
            .admit(
                queue.clone(),
                MutationSequencerKey::Compatible(MutationGenerationKind::Push),
                MutationIngress::Direct,
                Arc::new(push_work(1, "slot")),
                1,
                1,
            )
            .unwrap();
        let generation = sequencer.start_generation(&queue).unwrap();
        let members =
            validate_inert_mutation_generation(&snapshot, &[planned.request().as_ref().clone()])
                .unwrap();
        drop(permit);
        assert_eq!(slots.active(), 0);
        let prepared = retain_sequencer_after_slot_release(members, generation);
        assert!(prepared.slot_and_connection_released);
        assert_eq!(prepared.sequencer.requests().len(), 1);

        let queued_at = Instant::now() - Duration::from_secs(1);
        let expired = abort_unplanned_generation_on_deadline(
            Vec::<crate::MutationTicket<QueueKey, MutationSequencerKey, MutationGenerationWork>>::new(),
            queued_at,
            Duration::ZERO,
        );
        let Err(error) = expired else {
            panic!("queued generation deadline must reject");
        };
        assert_eq!(
            error,
            EngineError::Backpressure {
                resource: crate::MUTATION_SEQUENCER_WAIT_RESOURCE,
            }
        );

        let source = include_str!("async_composed.rs");
        let abort = source
            .split("pub fn abort_unplanned_generation_on_deadline<")
            .nth(1)
            .expect("deadline helper")
            .split("fn coordination_backpressure(")
            .next()
            .expect("deadline helper end");
        assert!(!abort.contains("loop"));
        assert!(!abort.contains("retry"));
        let serving_push = source
            .split("pub async fn push(")
            .nth(1)
            .expect("serving push")
            .split("pub async fn prepare_push(")
            .next()
            .expect("serving push end");
        assert!(serving_push.contains("planned_push(planner, request)"));
        assert!(!serving_push.contains("validate_inert_mutation_generation"));
        assert!(!serving_push.contains("SharedDriverReadAdmission"));
        assert!(!serving_push.contains("SelectionFence"));
        drop(tickets);
    }

    #[derive(Clone)]
    struct FixedOutcomeCommitter {
        calls: Arc<AtomicUsize>,
        outcome: EngineResult<RawCommitOutcome>,
    }

    impl UnifiedAtomicCommitter for FixedOutcomeCommitter {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;

        fn commit_atomic(&self, _request: Self::Request) -> OwnedTask<Self::Output> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let outcome = self.outcome.clone();
            Box::pin(async move { outcome })
        }
    }

    #[derive(Clone)]
    struct FixedRenderPlanner {
        item_id: ItemId,
        rendered: EngineResult<Vec<ClaimedItem>>,
        render_calls: Arc<AtomicUsize>,
    }

    impl AsyncClaimPlanner for FixedRenderPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            let item_id = self.item_id;
            Box::pin(async move {
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(
                        request.shard.clone(),
                        vec![claim_envelope(&request, item_id)],
                        1,
                    ),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            self.render_calls.fetch_add(1, Ordering::AcqRel);
            let rendered = self.rendered.clone();
            Box::pin(async move { rendered })
        }
    }

    #[test]
    fn durable_commit_outcome_and_render_failures_are_phase_aware() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let render_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::appended(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            FixedRenderPlanner {
                item_id,
                rendered: Ok(vec![claimed_item(&request, item_id)]),
                render_calls: Arc::clone(&render_calls),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request.clone()));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::AfterCommit {
                stage: AsyncClaimPostCommitStage::CommitOutcome,
                ..
            }))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);
        assert_eq!(render_calls.load(Ordering::Acquire), 0);

        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let mut wrong_item = claimed_item(&request, item_id);
        wrong_item.lease_expires_at = UtcTimestamp::new(99, 0).unwrap();
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            FixedRenderPlanner {
                item_id,
                rendered: Ok(vec![wrong_item]),
                render_calls: Arc::new(AtomicUsize::new(0)),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::AfterCommit {
                stage: AsyncClaimPostCommitStage::RenderValidation,
                ..
            }))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn render_storage_error_is_reported_after_commit() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            FixedRenderPlanner {
                item_id,
                rendered: Err(EngineError::Storage("render unavailable".to_string())),
                render_calls: Arc::new(AtomicUsize::new(0)),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::AfterCommit {
                stage: AsyncClaimPostCommitStage::Render,
                ..
            }))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);
    }

    #[derive(Clone)]
    struct CompatibilityBypassPlanner {
        empty: bool,
        plan_calls: Arc<AtomicUsize>,
        item_id: ItemId,
    }

    impl AsyncClaimPlanner for CompatibilityBypassPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            self.plan_calls.fetch_add(1, Ordering::AcqRel);
            let empty = self.empty;
            let item_id = self.item_id;
            Box::pin(async move {
                if empty {
                    return Ok(AsyncClaimPlan::empty());
                }
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(
                        request.shard.clone(),
                        vec![claim_envelope(&request, item_id)],
                        1,
                    ),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            panic!("invalid compatibility must fail before render")
        }
    }

    fn assert_invalid_compatibility_precedes_plan(compatibility: ClaimCompatibility, empty: bool) {
        let request = ClaimRequest {
            compatibility,
            ..claim_request("claim")
        };
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            CompatibilityBypassPlanner {
                empty,
                plan_calls: Arc::clone(&plan_calls),
                item_id: ItemId::mint(1, 1, 1),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::BeforeCommit(EngineError::Invalid(_))))
        ));
        assert_eq!(plan_calls.load(Ordering::Acquire), 0);
        assert_eq!(commit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn invalid_compatibility_cannot_return_empty_success() {
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                same_group_key: true,
                group_batching: Some(crate::GroupBatching { max_groups: 1 }),
                ..Default::default()
            },
            true,
        );
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                group_key: Some(fireweed_core::GroupKey::new("bad key!").unwrap()),
                ..Default::default()
            },
            true,
        );
    }

    #[test]
    fn lifecycle_commit_outcome_respects_the_durability_barrier() {
        let shard = queue("lifecycle");
        let position = CommandPosition::new(shard.clone(), 7, 1);
        let appended = RawCommitOutcome::appended(vec![position.clone()]);
        let applied = RawCommitOutcome::applied(vec![position.clone()]);

        assert!(
            validate_lifecycle_commit_outcome(
                DurabilityClass::EventualApply,
                &shard,
                7,
                &appended,
            )
            .is_ok()
        );
        assert!(
            validate_lifecycle_commit_outcome(DurabilityClass::Atomic, &shard, 7, &appended)
                .is_err()
        );
        assert!(
            validate_lifecycle_commit_outcome(DurabilityClass::Atomic, &shard, 7, &applied).is_ok()
        );
        assert!(
            validate_lifecycle_commit_outcome(DurabilityClass::EventualApply, &shard, 7, &applied,)
                .is_ok()
        );

        let wrong_queue =
            RawCommitOutcome::appended(vec![CommandPosition::new(queue("other"), 7, 1)]);
        assert!(
            validate_lifecycle_commit_outcome(
                DurabilityClass::EventualApply,
                &shard,
                7,
                &wrong_queue,
            )
            .is_err()
        );
        assert!(
            validate_lifecycle_commit_outcome(
                DurabilityClass::EventualApply,
                &shard,
                8,
                &RawCommitOutcome::appended(vec![position.clone()]),
            )
            .is_err()
        );
        assert!(
            validate_lifecycle_commit_outcome(
                DurabilityClass::EventualApply,
                &shard,
                7,
                &RawCommitOutcome::appended(vec![position.clone(), position]),
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_compatibility_cannot_reach_commit_plan() {
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                group_batching: Some(crate::GroupBatching { max_groups: 0 }),
                ..Default::default()
            },
            false,
        );
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                same_group_key: true,
                group_key: Some(fireweed_core::GroupKey::new("group").unwrap()),
                whole_cohort: true,
                ..Default::default()
            },
            false,
        );
    }

    #[test]
    fn claim_batch_limits_are_enforced_before_planning() {
        for max_items in [0, 101] {
            let fixture = claim_fixture(1, true, true);
            let mut request = claim_request("claim");
            request.max_items = max_items;
            let mut claim = Box::pin(fixture.backend.claim(request));
            assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
            assert!(fixture.dispatcher.drive_next());
            assert!(matches!(
                poll_once(claim.as_mut()),
                Poll::Ready(Err(AsyncClaimError::BeforeCommit(EngineError::Invalid(
                    "claim batch is outside queue limits"
                ))))
            ));
            assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
            assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn rejected_and_closed_operation_submission_never_invokes_planner() {
        let rejected = fixture(0);
        let effects = Arc::new(AtomicUsize::new(0));
        let rejected_effects = Arc::clone(&effects);
        let mut full = Box::pin(rejected.backend.submit_operation(queue("q"), move || {
            rejected_effects.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { queue("q") })
        }));
        assert!(matches!(
            poll_once(full.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));

        let closed = fixture(1);
        closed.dispatcher.close();
        let closed_effects = Arc::clone(&effects);
        let mut closed_submission =
            Box::pin(closed.backend.submit_operation(queue("q"), move || {
                closed_effects.fetch_add(1, Ordering::AcqRel);
                Box::pin(async { queue("q") })
            }));
        assert!(matches!(
            poll_once(closed_submission.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(DispatchError::Closed)))
        ));
        assert_eq!(effects.load(Ordering::Acquire), 0);
        assert_eq!(rejected.backend.admission.entry_count(), 0);
        assert_eq!(closed.backend.admission.entry_count(), 0);
    }

    #[test]
    fn caller_cancellation_after_operation_acceptance_does_not_cancel_work() {
        let fixture = fixture(1);
        let phase = Phase::new(false);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut caller = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                Arc::clone(&phase),
                planned,
                active,
                max_active,
                Arc::clone(&finished),
            ),
        ));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(caller);
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(finished.load(Ordering::Acquire), 1);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn rejected_lazy_factory_has_no_construction_effects() {
        let fixture = fixture(0);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Phase::new(true));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(
            poll_once(caller.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));
        assert_eq!(fixture.constructed.load(Ordering::Acquire), 0);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn started_caller_cancellation_does_not_cancel_accepted_task() {
        let fixture = fixture(1);
        let phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Arc::clone(&phase));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(caller);
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(fixture.completed.lock().unwrap().as_slice(), &[queue("q")]);
    }

    #[derive(Clone, Copy)]
    enum FixedReclaimMode {
        Valid,
        Empty,
        WrongEpoch,
        ReplayMetadata,
        PlannerError,
    }

    #[derive(Clone)]
    struct FixedReclaimPlanner {
        item_ids: Vec<ItemId>,
        calls: Arc<AtomicUsize>,
        mode: FixedReclaimMode,
    }

    impl AsyncReclaimPlanner for FixedReclaimPlanner {
        fn plan_reclaim(
            &self,
            request: AsyncReclaimRequest,
        ) -> OwnedTask<EngineResult<AsyncReclaimPlan>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let item_ids = self.item_ids.clone();
            let mode = self.mode;
            Box::pin(async move {
                if matches!(mode, FixedReclaimMode::PlannerError) {
                    return Err(EngineError::Storage("reclaim planning failed".to_string()));
                }
                if matches!(mode, FixedReclaimMode::Empty) {
                    return Ok(AsyncReclaimPlan::empty());
                }
                let mut envelope = CommandEnvelope {
                    command_id: CommandId::new("reclaim-command"),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids: item_ids.clone(),
                    command: QueueCommand::LeaseExpired(crate::LeaseExpiredCommand {
                        item_ids: item_ids.clone(),
                    }),
                    checksum: CommandChecksum(0),
                    created_at: request.now,
                };
                if matches!(mode, FixedReclaimMode::ReplayMetadata) {
                    envelope.request_fingerprint = Some(7);
                }
                let epoch = if matches!(mode, FixedReclaimMode::WrongEpoch) {
                    request.expected_epoch.unwrap_or(1) + 1
                } else {
                    request.expected_epoch.unwrap_or(1)
                };
                Ok(AsyncReclaimPlan::commit(
                    RawCommitRequest::new(request.shard, vec![envelope], epoch),
                    item_ids,
                ))
            })
        }
    }

    fn reclaim_request(name: &str) -> AsyncReclaimRequest {
        AsyncReclaimRequest {
            shard: queue(name),
            limit: Some(2),
            now: UtcTimestamp::new(30, 0).unwrap(),
            expected_epoch: Some(1),
        }
    }

    fn reclaim_items() -> Vec<ItemId> {
        vec![ItemId::mint(1, 1, 1), ItemId::mint(1, 1, 2)]
    }

    #[test]
    fn reclaim_happy_path_and_empty_batch_use_one_queue_operation() {
        for (mode, expected_commits, expected_items) in [
            (FixedReclaimMode::Valid, 1, reclaim_items()),
            (FixedReclaimMode::Empty, 0, Vec::new()),
        ] {
            let request = reclaim_request("reclaim");
            let calls = Arc::new(AtomicUsize::new(0));
            let commit_calls = Arc::new(AtomicUsize::new(0));
            let committer = FixedOutcomeCommitter {
                calls: Arc::clone(&commit_calls),
                outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                    request.shard.clone(),
                    1,
                    1,
                )])),
            };
            let strategy =
                UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
            let dispatcher = ControlledDispatcher::new(1);
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
                .with_reclaim_planner(FixedReclaimPlanner {
                    item_ids: reclaim_items(),
                    calls: Arc::clone(&calls),
                    mode,
                });

            let mut reclaim = Box::pin(backend.reclaim_expired(request));
            assert!(matches!(poll_once(reclaim.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            assert!(
                matches!(poll_once(reclaim.as_mut()), Poll::Ready(Ok(ids)) if ids == expected_items)
            );
            assert_eq!(calls.load(Ordering::Acquire), 1);
            assert_eq!(commit_calls.load(Ordering::Acquire), expected_commits);
        }
    }

    #[test]
    fn reclaim_fence_smuggling_and_planner_failures_stop_before_commit() {
        for mode in [
            FixedReclaimMode::WrongEpoch,
            FixedReclaimMode::ReplayMetadata,
            FixedReclaimMode::PlannerError,
        ] {
            let request = reclaim_request("reclaim");
            let commit_calls = Arc::new(AtomicUsize::new(0));
            let committer = FixedOutcomeCommitter {
                calls: Arc::clone(&commit_calls),
                outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                    request.shard.clone(),
                    1,
                    1,
                )])),
            };
            let strategy =
                UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
            let dispatcher = ControlledDispatcher::new(1);
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
                .with_reclaim_planner(FixedReclaimPlanner {
                    item_ids: reclaim_items(),
                    calls: Arc::new(AtomicUsize::new(0)),
                    mode,
                });

            let mut reclaim = Box::pin(backend.reclaim_expired(request));
            assert!(matches!(poll_once(reclaim.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            assert!(matches!(
                poll_once(reclaim.as_mut()),
                Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(_)))
            ));
            assert_eq!(commit_calls.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn reclaim_rejects_invalid_plan_footprints() {
        let request = reclaim_request("reclaim");
        let ids = reclaim_items();
        let envelope = |envelope_ids: Vec<ItemId>, command_ids: Vec<ItemId>| CommandEnvelope {
            command_id: CommandId::new("reclaim-command"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: envelope_ids,
            command: QueueCommand::LeaseExpired(crate::LeaseExpiredCommand {
                item_ids: command_ids,
            }),
            checksum: CommandChecksum(0),
            created_at: request.now,
        };

        let duplicate = vec![ids[0], ids[0]];
        let commit = RawCommitRequest::new(
            request.shard.clone(),
            vec![envelope(duplicate.clone(), duplicate.clone())],
            1,
        );
        assert!(validate_reclaim_plan(&request, &commit, &duplicate).is_err());

        let mut reversed = ids.clone();
        reversed.reverse();
        let commit = RawCommitRequest::new(
            request.shard.clone(),
            vec![envelope(ids.clone(), reversed)],
            1,
        );
        assert!(validate_reclaim_plan(&request, &commit, &ids).is_err());

        let commit = RawCommitRequest::new(
            request.shard.clone(),
            vec![envelope(ids.clone(), ids.clone())],
            1,
        )
        .with_fault(RawCommitFault::AfterAppendBeforeApply);
        assert!(validate_reclaim_plan(&request, &commit, &ids).is_err());
    }

    #[test]
    fn reclaim_commit_and_outcome_failures_are_phase_aware() {
        let request = reclaim_request("reclaim");
        for (outcome, expected_commit_error) in [
            (Err(EngineError::Storage("commit failed".to_string())), true),
            (
                Ok(RawCommitOutcome::appended(vec![CommandPosition::new(
                    request.shard.clone(),
                    1,
                    1,
                )])),
                false,
            ),
        ] {
            let committer = FixedOutcomeCommitter {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome,
            };
            let strategy =
                UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
            let dispatcher = ControlledDispatcher::new(1);
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
                .with_reclaim_planner(FixedReclaimPlanner {
                    item_ids: reclaim_items(),
                    calls: Arc::new(AtomicUsize::new(0)),
                    mode: FixedReclaimMode::Valid,
                });
            let mut reclaim = Box::pin(backend.reclaim_expired(request.clone()));
            assert!(matches!(poll_once(reclaim.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            let result = poll_once(reclaim.as_mut());
            if expected_commit_error {
                assert!(matches!(
                    result,
                    Poll::Ready(Err(AsyncLifecycleError::Commit(_)))
                ));
            } else {
                assert!(matches!(
                    result,
                    Poll::Ready(Err(AsyncLifecycleError::AfterCommit {
                        stage: AsyncLifecyclePostCommitStage::CommitOutcome,
                        ..
                    }))
                ));
            }
        }
    }

    #[test]
    fn reclaim_cancellation_does_not_cancel_commit_and_same_queue_is_serialized() {
        let phase = Phase::new(false);
        let completed = Arc::new(AtomicBool::new(false));
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: Arc::clone(&commit_calls),
                completed: Arc::clone(&completed),
                phase: Arc::clone(&phase),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(2);
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
            .with_reclaim_planner(FixedReclaimPlanner {
                item_ids: reclaim_items(),
                calls: Arc::clone(&plan_calls),
                mode: FixedReclaimMode::Valid,
            });

        let mut first = Box::pin(backend.reclaim_expired(reclaim_request("reclaim")));
        let mut second = Box::pin(backend.reclaim_expired(reclaim_request("reclaim")));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(dispatcher.accepted(), 1);
        assert!(dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        assert_eq!(plan_calls.load(Ordering::Acquire), 1);
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));

        drop(first);
        phase.release();
        assert!(dispatcher.drive_next());
        assert!(completed.load(Ordering::Acquire));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);

        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(dispatcher.accepted(), 1);
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(second.as_mut()),
            Poll::Ready(Ok(ids)) if ids == reclaim_items()
        ));
        assert_eq!(plan_calls.load(Ordering::Acquire), 2);
        assert_eq!(commit_calls.load(Ordering::Acquire), 2);
    }

    #[derive(Clone)]
    struct FixedCohortPlanner {
        item_ids: Vec<ItemId>,
        smuggle: bool,
    }

    impl AsyncCohortLifecyclePlanner for FixedCohortPlanner {
        fn plan_cohort_renew(
            &self,
            request: AsyncCohortRenewRequest,
        ) -> OwnedTask<EngineResult<crate::AsyncCohortLifecyclePlan>> {
            let item_ids = self.item_ids.clone();
            let smuggle = self.smuggle;
            Box::pin(async move {
                let envelope = CommandEnvelope {
                    command_id: CommandId::new("cohort-renew"),
                    request_id: None,
                    request_fingerprint: smuggle.then_some(7),
                    request_outcome: None,
                    item_ids: item_ids.clone(),
                    command: QueueCommand::CohortRenewLease(crate::CohortRenewLeaseCommand {
                        cohort_id: request.cohort_id,
                        lease_expires_at: request.new_lease_expires_at,
                    }),
                    checksum: CommandChecksum(0),
                    created_at: request.now,
                };
                Ok(crate::AsyncCohortLifecyclePlan::renew(
                    RawCommitRequest::new(
                        request.shard,
                        vec![envelope],
                        request.expected_epoch.unwrap_or(1),
                    ),
                    item_ids,
                ))
            })
        }

        fn plan_cohort_finalize(
            &self,
            request: AsyncCohortFinalizeRequest,
        ) -> OwnedTask<EngineResult<crate::AsyncCohortLifecyclePlan>> {
            let item_ids = self.item_ids.clone();
            Box::pin(async move {
                let applied_state = match request.kind {
                    FinalizeKind::Complete => fireweed_core::ItemState::Complete,
                    FinalizeKind::Fail => fireweed_core::ItemState::Failed,
                    FinalizeKind::Retry | FinalizeKind::Release => {
                        fireweed_core::ItemState::Pending
                    }
                    FinalizeKind::Rearm => fireweed_core::ItemState::Pending,
                };
                let outcomes = item_ids
                    .iter()
                    .copied()
                    .map(|item_id| crate::FinalizeOutcome {
                        item_id,
                        kind: request.kind,
                        applied_state: Some(applied_state),
                        not_before: request.not_before,
                    })
                    .collect::<Vec<_>>();
                let envelope = CommandEnvelope {
                    command_id: CommandId::new("cohort-finalize"),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids: item_ids.clone(),
                    command: QueueCommand::CohortFinalize(crate::CohortFinalizeCommand {
                        cohort_id: request.cohort_id,
                        kind: request.kind,
                        not_before: request.not_before,
                    }),
                    checksum: CommandChecksum(0),
                    created_at: request.now,
                };
                Ok(crate::AsyncCohortLifecyclePlan::finalize(
                    RawCommitRequest::new(
                        request.shard,
                        vec![envelope],
                        request.expected_epoch.unwrap_or(1),
                    ),
                    item_ids,
                    outcomes,
                ))
            })
        }
    }

    #[test]
    fn final_backend_shape_accepts_lifecycle_reclaim_and_cohort_planners_together() {
        let dispatcher = ControlledDispatcher::new(1);
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, AppendedOnlyCommitter)
                .unwrap();
        let _backend = AsyncComposedBackend::new(strategy, dispatcher, 1)
            .with_lifecycle_planner(ControlledLifecyclePlanner {
                smuggle: LifecycleSmuggle::None,
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .with_reclaim_planner(FixedReclaimPlanner {
                item_ids: reclaim_items(),
                calls: Arc::new(AtomicUsize::new(0)),
                mode: FixedReclaimMode::Valid,
            })
            .with_cohort_lifecycle_planner(FixedCohortPlanner {
                item_ids: reclaim_items(),
                smuggle: false,
            });
    }

    fn cohort_renew_request(name: &str) -> AsyncCohortRenewRequest {
        AsyncCohortRenewRequest {
            shard: queue(name),
            cohort_id: CohortId::new("cohort").unwrap(),
            cohort_lease_token: LeaseToken::new("cohort-token").unwrap(),
            new_lease_expires_at: UtcTimestamp::new(40, 0).unwrap(),
            now: UtcTimestamp::new(30, 0).unwrap(),
            expected_epoch: Some(1),
        }
    }

    #[test]
    fn typed_cohort_renew_routes_strategy_and_rejects_smuggling() {
        for (smuggle, expected_commits) in [(false, 1), (true, 0)] {
            let request = cohort_renew_request("cohort");
            let commit_calls = Arc::new(AtomicUsize::new(0));
            let strategy = UnifiedAtomicCommit::for_profile(
                DurabilityClass::Atomic,
                FixedOutcomeCommitter {
                    calls: Arc::clone(&commit_calls),
                    outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                        request.shard.clone(),
                        1,
                        1,
                    )])),
                },
            )
            .unwrap();
            let dispatcher = ControlledDispatcher::new(1);
            let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
                .with_cohort_lifecycle_planner(FixedCohortPlanner {
                    item_ids: reclaim_items(),
                    smuggle,
                });
            let mut renew = Box::pin(backend.renew_cohort(request));
            assert!(matches!(poll_once(renew.as_mut()), Poll::Pending));
            assert!(dispatcher.drive_next());
            if smuggle {
                assert!(matches!(
                    poll_once(renew.as_mut()),
                    Poll::Ready(Err(AsyncLifecycleError::BeforeCommit(_)))
                ));
            } else {
                assert!(matches!(poll_once(renew.as_mut()), Poll::Ready(Ok(()))));
            }
            assert_eq!(commit_calls.load(Ordering::Acquire), expected_commits);
        }
    }

    #[test]
    fn typed_cohort_finalize_survives_dropped_caller() {
        let phase = Phase::new(false);
        let completed = Arc::new(AtomicBool::new(false));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ClaimCommitter {
                calls: Arc::new(AtomicUsize::new(0)),
                completed: Arc::clone(&completed),
                phase: Arc::clone(&phase),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 2)
            .with_cohort_lifecycle_planner(FixedCohortPlanner {
                item_ids: reclaim_items(),
                smuggle: false,
            });
        let renew = cohort_renew_request("cohort");
        let request = AsyncCohortFinalizeRequest {
            shard: renew.shard,
            cohort_id: renew.cohort_id,
            cohort_lease_token: renew.cohort_lease_token,
            kind: FinalizeKind::Complete,
            not_before: None,
            now: renew.now,
            expected_epoch: renew.expected_epoch,
        };
        let mut finalize = Box::pin(backend.finalize_cohort(request));
        assert!(matches!(poll_once(finalize.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(finalize);
        phase.release();
        assert!(dispatcher.drive_next());
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn stalled_queue_does_not_block_actual_unrelated_queue_progress() {
        let fixture = fixture(2);
        let stalled_phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("a"), Arc::clone(&stalled_phase));
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("b"), Phase::new(true));
        let mut stalled = Box::pin(fixture.backend.submit_commit(request("a")));
        let mut unrelated = Box::pin(fixture.backend.submit_commit(request("b")));
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(stalled_phase.started.load(Ordering::Acquire));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Ready(Ok(key)) if key == queue("b")));
        assert_eq!(fixture.completed.lock().unwrap().as_slice(), &[queue("b")]);
        stalled_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn submit_close_is_linearizable_and_drain_waits_for_pending_task() {
        let fixture = fixture(2);
        let phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Arc::clone(&phase));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        fixture.backend.close();
        let effects = Arc::new(AtomicUsize::new(0));
        let rejected_effects = Arc::clone(&effects);
        assert!(matches!(
            fixture.dispatcher.submit(Box::new(move || {
                rejected_effects.fetch_add(1, Ordering::AcqRel);
                Box::pin(async { queue("never") })
            })),
            Err(DispatchError::Closed)
        ));
        assert_eq!(effects.load(Ordering::Acquire), 0);
        let mut drain = Box::pin(fixture.backend.drain());
        assert!(matches!(poll_once(drain.as_mut()), Poll::Pending));
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(drain.as_mut()), Poll::Ready(Ok(()))));
        assert_eq!(fixture.dispatcher.accepted(), 0);
    }
}
