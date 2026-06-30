//! # Orthogonal backend composition (ADR-012)
//!
//! A backend is the product `LogStore × ProjectionStore × ControlPlane`, assembled by ONE generic
//! [`ComposedBackend`]. The orchestration logic (claim/push/upsert/finalize/renew/reassign/purge/
//! update-fields/reclaim/tick) lives here ONCE, generically, instead of being duplicated in every
//! monolithic backend crate. A new backend is a new axis impl — a log, a projection, or a control
//! plane — not a new monolith, and it inherits the TD-001 conformance suite for free.
//!
//! ## The three axes
//!
//! - [`LogStore`] — the durable command log + the epoch/fence authority (co-located with the log,
//!   TD-003) + the replay cursor + snapshots + the `command_position` high-water.
//! - [`ProjectionStore`] — the materialized read model: the full read surface, the index queries, the
//!   pre-commit validation helpers, and the `apply` seam.
//! - [`ControlPlane`] — queue definitions + placement.
//!
//! ## The atomic write seam
//!
//! [`ComposedBackend`] owns `Mutex<Inner<L, P>>`; the log and projection substrates are disjoint fields
//! under one lock. Every write funnels through the single choke point [`ComposedBackend::commit_locked`],
//! which sequences `epoch-resolve → fence → log.append → projection.apply`. This is the SEPARATE-store
//! path (memory, sqlite-log-replay). The unified-transactional path (relational) reuses the same choke
//! point with a single transactional store implementing BOTH axes — see ADR-012 §"The atomic write seam".

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};

use crate::claim_validation::{ClaimCompatibility, require_item_level_claim};
use crate::command::{
    ClaimCommand, CommandChecksum, CommandEnvelope, CommandId, FinalizeCommand, FinalizeOutcome,
    LeaseExpiredCommand, PayloadUpdate, PurgeItemsCommand, PushCommand, PushItem, QueueCommand,
    QueueCounters, ReassignLeaseCommand, RenewLeaseCommand, ReplacePendingCommand,
    UpdateFieldsCommand, build_push_items, validate_gate_command, validate_gate_push,
};
use crate::error::{EngineError, EngineResult};
use crate::finalize_validation::validate_purge_force;
use crate::idempotency::{IdempotencyDecision, QueueIdempotencyCache};
use crate::port::{
    Backend, ClaimPort, ClaimRequest, Claimed, ClaimedItem, CommandPage, ControlPlaneStore,
    CreateQueueOutcome, FinalizePort, IndexHit, IndexQueryPort, ItemView, LeaseView, LiveItemView,
    LogRead, LogWriter, ProjectionRead, ProjectionSnapshot, ProjectionWriter, PurgePort, PushPort,
    PushSpec, QueueMetrics, ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort,
    SnapshotRef, SnapshotStore, TickReport, UpdateFieldsPort, UpsertOutcome, UpsertPort,
};
use crate::types::{CommandPosition, DurabilityClass, QueueKey};

// ---------------------------------------------------------------------------
// Axis 1: LogStore — the durable command log + epoch/fence authority
// ---------------------------------------------------------------------------

/// The command-log axis: the durable (or in-process) command log, the epoch/fence authority (TD-003,
/// co-located with the log), the replay cursor, the snapshots, and the persisted high-water mark.
///
/// The composition holds the substrate under its unit-of-work lock and calls these methods with `&mut`
/// (writes) / `&` (reads) WHILE the lock is held, so append+apply is one atomic unit of work. Object
/// safety is not required — the composition is generic (zero-cost, monomorphized).
pub trait LogStore: Send {
    /// Register a shard's log (called from `create_queue`). Idempotent.
    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()>;

    /// The current `assignment_epoch` for `shard` (the `backend_epoch` new positions carry). `NotFound`
    /// if the shard's log does not exist.
    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64>;

    /// Acquire a strictly-greater, durably-recorded `assignment_epoch` (TD-003 acquire). Returns the new
    /// epoch. `NotFound` if the shard's log does not exist.
    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64>;

    /// Append `commands` under `expected_epoch`, advancing the persisted high-water, returning the
    /// committed positions in order. Implements the TD-003 fencing rule: an `expected_epoch` that is not
    /// the log's current epoch is rejected with [`EngineError::EpochFenced`], appending nothing.
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>>;

    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> EngineResult<CommandPage>;

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>>;
    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()>;

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef>;
    fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>>;
    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot>;
}

// ---------------------------------------------------------------------------
// Axis 2: ProjectionStore — the materialized read model
// ---------------------------------------------------------------------------

/// The projection axis: the materialized read model. Exposes the full `ProjectionRead` surface, the
/// secondary-index queries, the pre-commit VALIDATION helpers the orchestration relies on (so the
/// post-append `apply` is infallible — commit has no rollback), and the `apply` seam itself.
///
/// All reads/validation are `&self`; `apply`/`ensure_shard` are `&mut self`. The composition calls these
/// under its UoW lock, so a claim's `select → append → apply → render` is one atomic unit.
pub trait ProjectionStore: Send {
    /// Materialize a shard's projection from its [`QueueDefinition`] (called from `create_queue`).
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()>;

    /// Apply committed `commands` (at `positions`) to the projection — the [`ProjectionWriter::apply`]
    /// seam. The caller pre-validated, so this is infallible in practice.
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()>;

    // -- claim / orchestration reads ----------------------------------------

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>>;
    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>>;
    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>>;
    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>>;
    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>>;
    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>>;
    /// Every shard's expired leases at `now` (the global `tick` sweep). Shards with none are omitted.
    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)>;

    // -- pre-commit validation ----------------------------------------------

    fn finalize_validate(&self, shard: &QueueKey, outcomes: &[FinalizeOutcome])
    -> EngineResult<()>;
    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()>;
    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()>;
    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()>;
    fn index_validate(
        &self,
        shard: &QueueKey,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()>;
    fn index_validate_push(&self, shard: &QueueKey, items: &[PushItem]) -> EngineResult<()>;
    fn index_validate_replace(
        &self,
        shard: &QueueKey,
        existing_id: &ItemId,
        item: &PushItem,
    ) -> EngineResult<()>;
    fn index_validate_update(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
    ) -> EngineResult<()>;

    // -- ProjectionRead surface ---------------------------------------------

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>>;
    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>>;
    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>>;
    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics>;
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>>;

    // -- secondary-index query ----------------------------------------------

    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>>;
    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>>;
}

// ---------------------------------------------------------------------------
// Axis 3: ControlPlane — queue definitions + placement
// ---------------------------------------------------------------------------

/// The control-plane axis: queue definitions + placement. The epoch is NOT here — it is the fence
/// authority and lives on the [`LogStore`] (ADR-012). For a postgres-native control plane that owns the
/// epoch transactionally, the `LogStore` facet forwards its epoch methods into this plane's transaction
/// (Phase 3+).
pub trait ControlPlane: Send + Sync {
    fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome>;
    fn queue_definition(&self, key: &QueueKey) -> EngineResult<QueueDefinition>;
    fn list_queues(&self, tenant: &TenantId) -> EngineResult<Vec<QueueId>>;
}

/// The in-process reference control plane: queue definitions in a `Mutex<HashMap>`. Used by the composed
/// memory and sqlite backends.
#[derive(Default)]
pub struct InProcessControlPlane {
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
}

impl InProcessControlPlane {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlPlane for InProcessControlPlane {
    fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome> {
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let mut g = self.queues.lock().expect("poisoned");
        if let Some(existing) = g.get(&key) {
            // Idempotent create: compatible iff the placement-identity fields match (API-001).
            if existing.ordering_mode != definition.ordering_mode
                || existing.priority_model != definition.priority_model
            {
                return Err(EngineError::QueueDefinitionConflict);
            }
            return Ok(CreateQueueOutcome {
                created: false,
                definition: existing.clone(),
            });
        }
        g.insert(key, definition.clone());
        Ok(CreateQueueOutcome {
            created: true,
            definition,
        })
    }

    fn queue_definition(&self, key: &QueueKey) -> EngineResult<QueueDefinition> {
        self.queues
            .lock()
            .expect("poisoned")
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound)
    }

    fn list_queues(&self, tenant: &TenantId) -> EngineResult<Vec<QueueId>> {
        Ok(self
            .queues
            .lock()
            .expect("poisoned")
            .keys()
            .filter(|k| k.tenant_id.as_str() == tenant.as_str())
            .map(|k| k.queue_id.clone())
            .collect())
    }
}

// ---------------------------------------------------------------------------
// ComposedBackend
// ---------------------------------------------------------------------------

/// The mutable substrate held under the composition's unit-of-work lock: the log + projection (disjoint
/// fields, so the UoW closure can borrow both `&mut`) + the per-queue request-id caches + the command
/// sequence.
struct Inner<L, P> {
    log: L,
    projection: P,
    idempotency: HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
    cmd_seq: u64,
}

/// The one generic backend (ADR-012): `Backend = LogStore × ProjectionStore × ControlPlane`. Implements
/// every engine port by delegating to the three axes.
pub struct ComposedBackend<L, P, C> {
    inner: Mutex<Inner<L, P>>,
    control: C,
    /// Packed into every minted [`ItemId`] (ADR-009) so concurrent writers never collide. `0` default.
    node_id: u8,
    counters: QueueCounters,
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ComposedBackend<L, P, C> {
    /// Assemble a backend from one of each axis.
    pub fn new(log: L, projection: P, control: C) -> Self {
        Self {
            inner: Mutex::new(Inner {
                log,
                projection,
                idempotency: HashMap::new(),
                cmd_seq: 0,
            }),
            control,
            node_id: 0,
            counters: QueueCounters::default(),
        }
    }

    /// Tag this backend with `node_id` — packed into the disambiguation byte of every minted [`ItemId`].
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    fn next_command_id(inner: &mut Inner<L, P>, node_id: u8) -> CommandId {
        let n = inner.cmd_seq;
        inner.cmd_seq += 1;
        CommandId::new(format!("cmp-{node_id}-{n}"))
    }

    fn make_envelope(
        inner: &mut Inner<L, P>,
        node_id: u8,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        let command_id = Self::next_command_id(inner, node_id);
        CommandEnvelope {
            command_id,
            request_id: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// The single atomic write choke point (ADR-012 §"The atomic write seam"): resolve the current epoch,
    /// fence the owner's cached epoch, append to the log, apply to the projection. Caller MUST pre-validate
    /// so the apply is infallible (commit has no rollback).
    fn commit_locked(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        env: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        validate_gate_command(false, &env.command)?;
        let epoch = inner.log.current_epoch(shard)?;
        // ADR-009 / TD-003: an owner that supplies its cached acquire-time epoch (`Some`) is fenced here if
        // superseded; `None` is the degenerate sole-owner path (stamp current, never fence).
        if expected_epoch.is_some_and(|e| e != epoch) {
            return Err(EngineError::EpochFenced);
        }
        let positions = inner.log.append(shard, std::slice::from_ref(&env), epoch)?;
        inner
            .projection
            .apply(&positions, std::slice::from_ref(&env))
    }

    fn max_attempts(&self, shard: &QueueKey) -> u32 {
        self.control
            .queue_definition(shard)
            .map(|d| d.retry_policy.max_attempts)
            .unwrap_or(1)
    }
}

/// `now + retention_ms` as the idempotency entry expiry.
fn request_expires_at(now: UtcTimestamp, retention_ms: u64) -> UtcTimestamp {
    let total = now.seconds as i128 * 1_000_000_000
        + now.nanoseconds as i128
        + retention_ms as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

/// Stable body fingerprint for request-id conflict detection (non-cryptographic hash over the serialized
/// push specs — determinism + collision-safety, not cryptographic strength).
fn push_body_hash(items: &[PushSpec]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

// ---------------------------------------------------------------------------
// UoW writer views (Backend::write) — disjoint borrows of log / projection
// ---------------------------------------------------------------------------

struct LogWriterView<'a, L> {
    log: &'a mut L,
}

impl<L: LogStore> LogWriter for LogWriterView<'_, L> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        for env in commands {
            validate_gate_command(false, &env.command)?;
        }
        self.log.append(shard, commands, expected_epoch)
    }
}

struct ProjectionWriterView<'a, P> {
    projection: &'a mut P,
}

impl<P: ProjectionStore> ProjectionWriter for ProjectionWriterView<'_, P> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.projection.apply(positions, commands)
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> Backend for ComposedBackend<L, P, C> {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        let result = {
            let mut g = self.inner.lock().expect("composed backend poisoned");
            let Inner {
                log, projection, ..
            } = &mut *g;
            let mut lw = LogWriterView { log };
            let mut pw = ProjectionWriterView { projection };
            f(&mut lw, &mut pw)
        };
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// ControlPlaneStore — queue defs delegate to C; epoch delegates to L (ADR-012)
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ControlPlaneStore
    for ComposedBackend<L, P, C>
{
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = (|| {
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let outcome = self.control.create_queue(definition)?;
            if outcome.created {
                let mut g = self.inner.lock().expect("poisoned");
                let Inner {
                    log, projection, ..
                } = &mut *g;
                log.ensure_shard(&key)?;
                projection.ensure_shard(&outcome.definition)?;
            }
            Ok(outcome)
        })();
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        std::future::ready(self.control.queue_definition(key))
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        std::future::ready(self.control.list_queues(tenant))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .current_epoch(shard);
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .acquire_epoch(shard);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// PushPort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> PushPort for ComposedBackend<L, P, C> {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let max_attempts = self.max_attempts(shard);
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let mut g = self.inner.lock().expect("poisoned");
            g.projection.index_validate_push(shard, &push_items)?;
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            let def = self.control.queue_definition(shard)?;
            let max_attempts = def.retry_policy.max_attempts;
            let expires_at = request_expires_at(now, def.request_id_retention_ms);
            let mut g = self.inner.lock().expect("poisoned");
            match g.idempotency.entry(shard.clone()).or_default().check(
                &request_id,
                fingerprint,
                now,
            ) {
                IdempotencyDecision::Replay(ids) => return Ok(ids),
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
            }
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            g.projection.index_validate_push(shard, &push_items)?;
            let command_id = Self::next_command_id(&mut g, self.node_id);
            let env = CommandEnvelope {
                command_id,
                request_id: Some(request_id.clone()),
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            g.idempotency.entry(shard.clone()).or_default().record(
                request_id,
                fingerprint,
                ids.clone(),
                expires_at,
            );
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// ClaimPort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ClaimPort for ComposedBackend<L, P, C> {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            // Resolve the claim unit from the compatibility options. Item-level (the default) is unchanged;
            // this log-replay composition refuses richer claim units with `Unavailable` rather than
            // silently downgrading them (BQ-14a).
            if req.compatibility != ClaimCompatibility::default() {
                let def = self.control.queue_definition(&req.shard)?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, &def)?;
            }
            let mut g = self.inner.lock().expect("poisoned");
            let candidates =
                g.projection
                    .eligible_candidates(&req.shard, req.now, req.max_items)?;
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                }),
                candidates.clone(),
                req.now,
            );
            Self::commit_locked(&mut g, &req.shard, env, req.expected_epoch)?;
            let items = g.projection.render_claimed(&req.shard, &candidates)?;
            debug_assert_eq!(
                items.len(),
                candidates.len(),
                "leased candidate failed to render"
            );
            Ok(Claimed {
                items,
                ..Default::default()
            })
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// UpsertPort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> UpsertPort for ComposedBackend<L, P, C> {
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let max_attempts = self.max_attempts(shard);
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, 1);
            let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id,
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: entity,
            };
            let mut g = self.inner.lock().expect("poisoned");
            let existing = g.projection.lookup_by_key(shard, client_item_key)?;
            match existing {
                None => {
                    g.projection
                        .index_validate(shard, &item.item_id, &item.fields, None)?;
                    let env = Self::make_envelope(
                        &mut g,
                        self.node_id,
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        vec![new_item_id],
                        now,
                    );
                    Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some(existing_id) => {
                    let state = g
                        .projection
                        .item_state(shard, &existing_id)?
                        .ok_or(EngineError::NotFound)?;
                    match state {
                        ItemState::Pending => {
                            g.projection
                                .index_validate_replace(shard, &existing_id, &item)?;
                            let env = Self::make_envelope(
                                &mut g,
                                self.node_id,
                                QueueCommand::ReplacePending(ReplacePendingCommand {
                                    client_item_key: client_item_key.clone(),
                                    superseded_item_id: existing_id,
                                    replacement: item,
                                }),
                                vec![new_item_id],
                                now,
                            );
                            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                            Ok(UpsertOutcome::Replaced {
                                new_item_id,
                                superseded_item_id: existing_id,
                            })
                        }
                        ItemState::Leased => {
                            Err(EngineError::Invalid("collision with claimed item"))
                        }
                        ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                    }
                }
            }
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// FinalizePort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> FinalizePort for ComposedBackend<L, P, C> {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            g.projection.finalize_validate(shard, &outcomes)?;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// RenewLeasePort / ReassignLeasePort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> RenewLeasePort for ComposedBackend<L, P, C> {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            g.projection.renew_validate(shard, &item_ids)?;
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ReassignLeasePort
    for ComposedBackend<L, P, C>
{
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            g.projection.reassign_validate(shard, &item_ids)?;
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// PurgePort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> PurgePort for ComposedBackend<L, P, C> {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-commit: enforce the force gate per id (a leased item needs force) and collect the ids
            // actually present (absent ids are no-ops, like Redis XDEL). De-dup so a repeated id counts once.
            let mut present: Vec<ItemId> = Vec::new();
            for id in &item_ids {
                if present.contains(id) {
                    continue;
                }
                if let Some(state) = g.projection.item_state(shard, id)? {
                    validate_purge_force(state == ItemState::Leased, force)?;
                    present.push(*id);
                }
            }
            if present.is_empty() {
                return Ok(0);
            }
            let count = present.len() as u64;
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::PurgeItems(PurgeItemsCommand {
                    item_ids: present.clone(),
                    force,
                }),
                present,
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(count)
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// UpdateFieldsPort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> UpdateFieldsPort
    for ComposedBackend<L, P, C>
{
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            g.projection
                .update_fields_validate(shard, &item_id, expected_item_version)?;
            g.projection
                .index_validate_update(shard, &item_id, &field_ops)?;
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops,
                    payload,
                    set_priority: Default::default(),
                    set_not_before: Default::default(),
                    set_entity_document: entity,
                }),
                vec![item_id],
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            g.projection
                .item_version(shard, &item_id)?
                .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// ReclaimPort / ReclaimDriver
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ReclaimPort for ComposedBackend<L, P, C> {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let mut ids = g.projection.expired_leases(shard, now)?;
            if let Some(limit) = limit {
                ids.truncate(limit);
            }
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                }),
                ids.clone(),
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ReclaimDriver for ComposedBackend<L, P, C> {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let expired = g.projection.all_expired_leases(now);
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                let env = Self::make_envelope(
                    &mut g,
                    self.node_id,
                    QueueCommand::LeaseExpired(LeaseExpiredCommand {
                        item_ids: ids.clone(),
                    }),
                    ids.clone(),
                    now,
                );
                Self::commit_locked(&mut g, &shard, env, None)?;
                report.leases_reclaimed += ids.len() as u64;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// LogRead
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> LogRead for ComposedBackend<L, P, C> {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .read_from(shard, from, limit);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// ProjectionRead
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ProjectionRead for ComposedBackend<L, P, C> {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .select_eligible(shard, now, limit);
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .peek(shard, limit);
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .pending(shard);
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .render_claimed(shard, ids);
        std::future::ready(result)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .live_items(shard, keys);
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .metrics(queue);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// IndexQueryPort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> IndexQueryPort for ComposedBackend<L, P, C> {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .index_get_unique(shard, index, key);
        std::future::ready(result)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .index_lookup(shard, index, key);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// SnapshotStore
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> SnapshotStore for ComposedBackend<L, P, C> {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .write_snapshot(shard, position, snapshot);
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .latest_snapshot(shard);
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .read_snapshot(snapshot_ref);
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let result = self.inner.lock().expect("poisoned").log.high_water(shard);
        std::future::ready(result)
    }

    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .set_high_water(shard, position);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Default-impl ports (relational-/commit-class features the log-replay composition refuses). These keep
// ComposedBackend wirable into the LibBackend bound; each inherits the `Unavailable` default.
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::SetGatesPort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::DiscoveryPort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::ReschedulePort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::CommitTransitionPort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::RecoveryReadPort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::CohortFinalizePort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::CohortRenewLeasePort
    for ComposedBackend<L, P, C>
{
}
