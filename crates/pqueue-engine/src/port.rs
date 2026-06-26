//! Driven and driving ports (TD-007 §2, plan §2.1).
//!
//! Hexagonal: these traits are defined by the domain and implemented by adapters. The engine
//! depends on nothing outward. Write-side ports (`LogWriter`/`ProjectionWriter`) are **sync** and
//! run inside a `Backend::write` unit of work; read/claim/reclaim ports are **async** (a backend
//! such as postgres is async). Atomicity for async backends is provided via `ClaimPort`/`UpsertPort`
//! (TD-007 §2.3), so the sync UoW closure suffices for the atomic-sync backends (memory, sqlite).

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition, QueueId, TenantId,
    UtcTimestamp, WorkerId,
};

use crate::claim_validation::ClaimCompatibility;
use crate::command::{CommandEnvelope, CommandId, FinalizeOutcome};
use crate::error::EngineResult;
use crate::types::{CommandPosition, DurabilityClass, QueueKey};

// ---------------------------------------------------------------------------
// Write side (sync; runs inside a Backend unit of work)
// ---------------------------------------------------------------------------

/// Appends commands to the durable log within the current unit of work.
pub trait LogWriter {
    /// Append `commands` to `shard`'s log, returning their committed positions in order.
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>>;
}

/// Applies committed commands to the projection within the current unit of work.
pub trait ProjectionWriter {
    /// Apply `commands` (already appended at `positions`) to the projection.
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()>;
}

// ---------------------------------------------------------------------------
// Backend: the atomic seam
// ---------------------------------------------------------------------------

/// A driven backend providing the atomic append+apply unit of work and read access.
///
/// On the `Atomic` class the closure's log append and projection apply commit together. On
/// `EventualApply` the closure has self-read-after-write only (TD-007 §2.1-2.2).
pub trait Backend: Send + Sync {
    fn durability_class(&self) -> DurabilityClass;

    /// Run `f` as one unit of work. The closure is synchronous (no `.await` inside); the async
    /// boundary is the method itself.
    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send;
}

// ---------------------------------------------------------------------------
// Read side (async)
// ---------------------------------------------------------------------------

/// A page of committed commands for replay/rebuild.
#[derive(Debug, Clone)]
pub struct CommandPage {
    pub entries: Vec<(CommandPosition, CommandEnvelope)>,
    pub next: Option<CommandPosition>,
}

pub trait LogRead: Send + Sync {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send;
}

/// A non-destructive view of an eligible item (RESP `peek` / library read).
#[derive(Debug, Clone)]
pub struct ItemView {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub priority: Option<PriorityValue>,
    pub item_version: u64,
}

/// A view of an in-flight (leased) item (RESP `XPENDING` / library read).
#[derive(Debug, Clone)]
pub struct LeaseView {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    pub attempt_count: u32,
}

/// Lifecycle counts + bound metrics (RESP `XLEN`/`XINFO` basic; rich is library-only).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueMetrics {
    pub pending: u64,
    pub leased: u64,
    pub complete: u64,
    pub failed: u64,
}

pub trait ProjectionRead: Send + Sync {
    /// Priority-ordered eligible candidates (Eligibility Precedence, API-001). The claim path
    /// leases from these in the same unit of work (Invariant 1: per-item delivery, no cursor).
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send;

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send;

    /// Render the rich claimed-item shape for specific (currently-leased) `ids` — the RESP `XCLAIM` reply
    /// (and any read that needs an in-flight item's full payload/fields, not just the [`LeaseView`]).
    /// Ids that are absent or not in a renderable state are silently omitted (the caller knows the set it
    /// just acted on).
    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send;

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send;
}

// ---------------------------------------------------------------------------
// Claim & upsert (atomic with selection)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ClaimRequest {
    pub shard: QueueKey,
    pub worker_id: WorkerId,
    pub max_items: usize,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    pub now: UtcTimestamp,
    /// API-001 Batch Claim compatibility options (group_key / same_group_key / metadata_equals /
    /// group_batching / whole_cohort). `ClaimCompatibility::default()` is an item-level claim
    /// ([`ClaimUnit::Item`](crate::ClaimUnit)) — backends resolve the unit via
    /// [`require_item_level_claim`](crate::require_item_level_claim) and (BQ-14a) admit Item; the
    /// group/cohort selection units land in BQ-14b/c.
    pub compatibility: ClaimCompatibility,
}

/// A claimed item in the API-001 claimed-item shape (lease fields included).
///
/// `metadata`/`gate_keys` are intentionally deferred (library-only render concerns); `group_key`,
/// `not_before`, and `attempt_count` are core data-model fields included now so adapters built on
/// this shape don't force a breaking widening later (review I2/I3).
#[derive(Debug, Clone)]
pub struct ClaimedItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    /// Delivery/reclaim count as of this claim (RESP delivery-count semantics; flavor-diff 7).
    pub attempt_count: u32,
    pub payload: Option<Bytes>,
}

#[derive(Debug, Clone, Default)]
pub struct Claimed {
    pub items: Vec<ClaimedItem>,
}

/// A backend that leases candidates atomically with selection (TD-007 §2.2). The engine is the
/// single *logical* claim authority; a backend MAY implement claim in one transaction.
pub trait ClaimPort: Send + Sync {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send;
}

/// Result of `replace_if_pending` (Invariant 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// No collision: a new item was appended.
    Inserted { item_id: ItemId },
    /// Colliding pending item atomically superseded; the new monotonic id is returned.
    Replaced {
        new_item_id: ItemId,
        superseded_item_id: ItemId,
    },
}

/// Pending-item replacement, executed in the **same unit of work as claim** so upsert and claim on
/// one item mutually exclude (TD-007 §2.3). Atomic class only; on eventual-apply the engine returns
/// `EngineError::Unavailable` without calling this port.
pub trait UpsertPort: Send + Sync {
    /// Upsert on `client_item_key`. The backend ASSIGNS the new item id from its own command sequence
    /// (restart-safe, unique across handles — like [`PushPort`]) and returns it in the `UpsertOutcome`;
    /// callers never supply an id (that would collide across two servers/handles on one backend).
    #[allow(clippy::too_many_arguments)]
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send;
}

/// A new-item spec for [`PushPort`]. The backend assigns the `item_id` (unique + restart-safe via its
/// own command sequence — NOT a caller-side counter, so two handles / a restart can't collide); the
/// dedup `client_item_key` defaults to that id (a unique append) when `None`.
#[derive(Debug, Clone, Default)]
pub struct PushSpec {
    pub client_item_key: Option<ClientItemKey>,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub payload: Option<Bytes>,
    /// Declared cohort size (BQ-14c) — see [`crate::PushItem::cohort_size`]. `None` for non-cohort items.
    pub cohort_size: Option<u64>,
}

/// Appends new items (server-assigned ids). The backend builds the envelope from its own command
/// sequence and commits through its atomic append+apply UoW after confirming the shard exists, so a
/// Push can never leave the log ahead of the projection (divergence-safe) and ids are unique across
/// handles + restart. The library facade's `push` routes here rather than reaching for `Backend::write`.
pub trait PushPort: Send + Sync {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;
}

/// Extends the lease on in-flight items, atomically pre-validating exactly like [`FinalizePort`]: a
/// fenced lease → `StaleLease`, a superseded id → `Superseded`, terminal → `Terminal`, non-leased →
/// `Invalid`, and the `RenewLease` command is NOT appended on rejection (no divergence). Lets a long-
/// running worker extend its lease without surrendering the claim.
pub trait RenewLeasePort: Send + Sync {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

/// Transfer an in-flight lease to a NEW consumer (RESP cross-consumer `XCLAIM`): swap the lease token and
/// charge exactly one delivery. Pre-validated identically to renew (`reassign_validate`): the items must
/// be Leased + not fenced/superseded/terminal, else a structured rejection with NOTHING appended. The
/// same-consumer case (token unchanged) is a no-charge [`RenewLeasePort::renew`] instead.
pub trait ReassignLeasePort: Send + Sync {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

/// Hard-delete specific items (RESP `XDEL`, operator/library purge). Returns the count actually removed
/// (ids absent from the projection are no-ops, like Redis `XDEL`). The `PurgeItems` apply is infallible
/// (remove-if-present), so the only pre-commit check is the API-001 force gate: purging a **leased** item
/// requires `force` (else `EngineError::Conflict`, nothing appended). `XDEL` passes `force = true`
/// (Redis deletes unconditionally); a library purge may pass `force = false` to honor the gate.
pub trait PurgePort: Send + Sync {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;
}

/// Finalizes claimed items (complete/fail/retry/release/rearm), atomically validating the lease
/// before committing: an **operator-fenced** lease is rejected with `EngineError::StaleLease` and the
/// Finalize command is NOT appended (no log/projection divergence; the fencing check is pre-commit).
/// Batch is all-or-nothing in this launch slice: any fenced item fails the whole call (per-item
/// results are a later refinement).
pub trait FinalizePort: Send + Sync {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// Clock, IdGen, ReclaimDriver
// ---------------------------------------------------------------------------

/// Injected clock — keeps the engine deterministic/testable.
pub trait Clock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}

/// Injected id generation.
pub trait IdGen: Send + Sync {
    fn next_item_id(&self) -> ItemId;
    fn next_command_id(&self) -> CommandId;
}

/// What a `tick` fired (TD-007 §3). Empty when nothing was due.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub leases_reclaimed: u64,
    pub cohorts_expired: u64,
    pub items_promoted: u64,
    pub progress_bound_breaches: u64,
}

impl TickReport {
    pub fn is_empty(&self) -> bool {
        *self == TickReport::default()
    }
}

/// Fires timed lifecycle transitions (lease expiry, cohort timeout, not_before/recurrence
/// promotion, progress-bound metering). The *logic* is domain; the *clock* is the composition
/// root's. `tick(now)` is idempotent (re-running at the same/earlier `now` makes no further
/// transitions) and serializes against claim via the same unit of work (TD-007 §3).
pub trait ReclaimDriver: Send + Sync {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send;
}

// ---------------------------------------------------------------------------
// Control plane: queue definitions + epoch source (plan §2.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CreateQueueOutcome {
    /// `false` for a compatible idempotent re-create (API-001).
    pub created: bool,
    pub definition: QueueDefinition,
}

/// Stores queue definitions and supplies the `backend_epoch` that `CommandPosition` carries and that
/// lease/gate fencing keys off (TD-003). At launch (single shard) the epoch is shard-local.
pub trait ControlPlaneStore: Send + Sync {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send;

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send;

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send;

    /// The current assignment epoch for `shard` (the `backend_epoch` of new positions).
    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;
}

// ---------------------------------------------------------------------------
// Snapshot store: replay acceleration + the persisted command_position high-water (TD-007 §4)
// ---------------------------------------------------------------------------

/// Serialized projection snapshot payload (opaque to the engine).
#[derive(Debug, Clone)]
pub struct ProjectionSnapshot {
    pub payload: Vec<u8>,
}

/// A reference to a written snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
    pub queue: QueueKey,
    pub position: CommandPosition,
    pub ref_id: String,
}

/// Persists projection snapshots and — crucially — the `command_position` **high-water mark**, so
/// replay after retention/compaction is monotonic and `item_version` never regresses (TD-007 §4).
/// The high-water mark is read from here, never recomputed by counting a (possibly compacted) log.
pub trait SnapshotStore: Send + Sync {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send;

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send;

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send;

    /// The persisted monotonic `command_position` high-water for `shard` (TD-007 §4).
    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send;

    /// Advance the persisted high-water mark. MUST be monotonic (reject a lower position).
    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}
