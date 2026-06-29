//! Driven and driving ports (TD-007 §2, plan §2.1).
//!
//! Hexagonal: these traits are defined by the domain and implemented by adapters. The engine
//! depends on nothing outward. Write-side ports (`LogWriter`/`ProjectionWriter`) are **sync** and
//! run inside a `Backend::write` unit of work; read/claim/reclaim ports are **async** (a backend
//! such as postgres is async). Atomicity for async backends is provided via `ClaimPort`/`UpsertPort`
//! (TD-007 §2.3), so the sync UoW closure suffices for the atomic-sync backends (memory, sqlite).

use std::collections::BTreeMap;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp, WorkerId,
};

use crate::claim_validation::ClaimCompatibility;
use crate::command::{
    CommandEnvelope, CommandId, FinalizeKind, FinalizeOutcome, SetGatesCommand, SideRecord,
};
use crate::error::{EngineError, EngineResult};
use crate::types::{CommandPosition, DurabilityClass, QueueKey};

// ---------------------------------------------------------------------------
// Write side (sync; runs inside a Backend unit of work)
// ---------------------------------------------------------------------------

/// Appends commands to the durable log within the current unit of work.
pub trait LogWriter {
    /// Append `commands` to `shard`'s log under the owner's `expected_epoch`, returning the committed
    /// positions in order. Implements the TD-003 Single Authoritative Fencing Rule, step 2: the append
    /// MUST reject any `expected_epoch` that is not the queue's current durable `assignment_epoch` (not
    /// merely `<=`) with [`EngineError::EpochFenced`](crate::EngineError::EpochFenced) — a superseded
    /// owner is fenced the instant a newer epoch is acquired, before any new-epoch segment exists. The
    /// committed positions carry the current epoch as their `backend_epoch`. An in-process owner passes the
    /// epoch it **cached at `acquire_queue_lease`** (ADR-009 / TD-003 In-Process Library Owner-Runtime), so a
    /// superseded owner self-fences here; a sole-owner / degenerate caller passes the current epoch and never
    /// fences. (The cached epoch is threaded from the data-plane ports as `expected_epoch: Option<u64>`, where
    /// `None` selects the always-current degenerate path.)
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
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

    /// Whether this backend stores gate membership and enforces `SetGates` at claim selection.
    fn supports_gates(&self) -> bool {
        false
    }

    /// The authoritative-commit capability descriptors (Snorri StateStore boundary, epic pqueue-2201fd37).
    /// Default = [`CommitCapabilities::default`] (all-false): a backend that has not wired the atomic commit
    /// boundary advertises NO commit guarantees, so a consumer rejects it before activation. Memory +
    /// sqlite-relational override this to advertise what they actually implement.
    fn commit_capabilities(&self) -> CommitCapabilities {
        CommitCapabilities::default()
    }

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

/// A live item addressed by `client_item_key`.
///
/// "Live" means still owned by the queue as active work: pending or leased, not terminal and not
/// superseded. The view intentionally includes the existing opaque payload plus the structured field map
/// so pqueue can serve as hot storage for compound work records without forcing callers to maintain a
/// second snapshot store.
#[derive(Debug, Clone)]
pub struct LiveItemView {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub lifecycle_state: ItemState,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub attempt_count: u32,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
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

    /// Render live hot-storage items by client key, preserving input order. A missing, terminal, purged,
    /// or superseded item renders as `None`; leased items are still live and render normally.
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send;

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send;
}

// ---------------------------------------------------------------------------
// Secondary-index query (ADR-010): exact composite-key lookup over configured item fields
// ---------------------------------------------------------------------------

/// One hit from a secondary-index lookup — enough to identify and re-read the item. Always carries the
/// item's CURRENT `item_version` (read-after-write).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHit {
    pub client_item_key: ClientItemKey,
    pub item_id: ItemId,
    pub item_version: u64,
}

/// Read port for per-queue secondary indexes (ADR-010 §6). The `key` is the per-field value bytes in
/// field order; the port encodes the §4.1 composite key and probes the index. The in-memory log-replay
/// family implements this over its shared `ProjectionData`; the relational family returns
/// [`EngineError::Unavailable`](crate::EngineError::Unavailable) until Phase 2 wires the side index table.
#[doc(hidden)]
pub trait IndexQueryPort: Send + Sync {
    /// Exact composite-key get on a UNIQUE index. `Ok(None)` if no item holds the key;
    /// [`EngineError::Invalid`](crate::EngineError::Invalid) if `index` is not a unique index on this queue.
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send;

    /// Exact composite-key lookup on a (non-unique or unique) index. Returns all matching items ordered
    /// by `item_id` ascending; empty if none.
    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send;
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
    /// The owner's cached acquire-time fence epoch (ADR-009 / TD-003 In-Process Library Owner-Runtime).
    /// `Some(e)` ⇒ the claim's atomic commit is fenced against `e`: if `e` is not the queue's current
    /// durable epoch (the owner has been superseded), the claim is rejected `EpochFenced` at commit and
    /// NOTHING is leased. `None` ⇒ the degenerate sole-owner path: stamp the current epoch, never fence
    /// (behaviour-preserving). The epoch MUST be the value cached at `acquire_queue_lease`, never re-read
    /// from `current_epoch` (re-reading defeats the fence).
    pub expected_epoch: Option<u64>,
}

/// A claimed item in the API-001 claimed-item shape (lease fields included).
///
/// `metadata`, `group_key`, `not_before`, `gate_keys`, and `attempt_count` are core data-model fields
/// included so adapters built on this shape don't force a breaking widening later (review I2/I3).
#[derive(Debug, Clone)]
pub struct ClaimedItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub lease_token: Option<LeaseToken>,
    pub lease_expires_at: UtcTimestamp,
    /// Delivery/reclaim count as of this claim (RESP delivery-count semantics; flavor-diff 7).
    pub attempt_count: u32,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    pub gate_keys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Claimed {
    pub items: Vec<ClaimedItem>,
    pub cohort_lease_token: Option<LeaseToken>,
    pub cohort_id: Option<CohortId>,
}

#[derive(Debug, Clone)]
pub struct CohortLeaseTarget {
    pub cohort_id: CohortId,
    pub cohort_lease_token: LeaseToken,
}

/// A backend that leases candidates atomically with selection (TD-007 §2.2). The engine is the
/// single *logical* claim authority; a backend MAY implement claim in one transaction.
#[doc(hidden)]
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
#[doc(hidden)]
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
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send;
}

/// A new-item spec for [`PushPort`]. The backend assigns the `item_id` (unique + restart-safe via its
/// own command sequence — NOT a caller-side counter, so two handles / a restart can't collide); the
/// dedup `client_item_key` defaults to that id (a unique append) when `None`.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PushSpec {
    pub client_item_key: Option<ClientItemKey>,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub payload: Option<Bytes>,
    /// Structured hot-storage fields for compound work records. These are item-local, mutable by
    /// replacement/upsert, and exposed through Redis-hash-shaped live read commands.
    pub fields: BTreeMap<String, Bytes>,
    /// Caller-owned item metadata used by API-001 compatibility predicates and returned verbatim in the
    /// claimed-item shape. pqueue stores and filters it without interpreting application meaning.
    pub metadata: Metadata,
    /// Declared cohort size (BQ-14c) — see [`crate::PushItem::cohort_size`]. `None` for non-cohort items.
    pub cohort_size: Option<u64>,
    /// Gate keys this item carries (BQ-14d) — see [`crate::PushItem::gate_keys`]. Empty for un-gated items.
    pub gate_keys: Vec<String>,
}

/// Appends new items (server-assigned ids). The backend builds the envelope from its own command
/// sequence and commits through its atomic append+apply UoW after confirming the shard exists, so a
/// Push can never leave the log ahead of the projection (divergence-safe) and ids are unique across
/// handles + restart. The library facade's `push` routes here rather than reaching for `Backend::write`.
#[doc(hidden)]
pub trait PushPort: Send + Sync {
    /// `expected_epoch`: the owner's cached acquire-time fence epoch (ADR-009 / TD-003). `Some(e)` fences the
    /// append at commit (a superseded owner → `EpochFenced`, nothing appended); `None` is the degenerate
    /// sole-owner path (stamp current, never fence).
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;

    /// Same append operation, but carrying API-001's envelope-level `request_id`. Backends that have not
    /// implemented durable request replay return `Unavailable` rather than silently accepting a request id
    /// without idempotency semantics.
    fn push_with_request_id(
        &self,
        _shard: &QueueKey,
        _request_id: RequestId,
        _items: Vec<PushSpec>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Operator gate-state mutation. Gate support is backend-capability-specific: relational backends
/// enforce it, while log-replay backends reject it before the command is appended.
#[doc(hidden)]
pub trait SetGatesPort: Send + Sync {
    fn set_gates(
        &self,
        _shard: &QueueKey,
        _command: SetGatesCommand,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Extends the lease on in-flight items, atomically pre-validating exactly like [`FinalizePort`]: a
/// fenced lease → `StaleLease`, a superseded id → `Superseded`, terminal → `Terminal`, non-leased →
/// `Invalid`, and the `RenewLease` command is NOT appended on rejection (no divergence). Lets a long-
/// running worker extend its lease without surrendering the claim.
#[doc(hidden)]
pub trait RenewLeasePort: Send + Sync {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

#[doc(hidden)]
pub trait CohortRenewLeasePort: Send + Sync {
    fn renew_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let _ = (shard, target, new_lease_expires_at, now, expected_epoch);
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Transfer an in-flight lease to a NEW consumer (RESP cross-consumer `XCLAIM`): swap the lease token and
/// charge exactly one delivery. Pre-validated identically to renew (`reassign_validate`): the items must
/// be Leased + not fenced/superseded/terminal, else a structured rejection with NOTHING appended. The
/// same-consumer case (token unchanged) is a no-charge [`RenewLeasePort::renew`] instead.
#[doc(hidden)]
pub trait ReassignLeasePort: Send + Sync {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

/// Hard-delete specific items (RESP `XDEL`, operator/library purge). Returns the count actually removed
/// (ids absent from the projection are no-ops, like Redis `XDEL`). The `PurgeItems` apply is infallible
/// (remove-if-present), so the only pre-commit check is the API-001 force gate: purging a **leased** item
/// requires `force` (else `EngineError::Conflict`, nothing appended). `XDEL` passes `force = true`
/// (Redis deletes unconditionally); a library purge may pass `force = false` to honor the gate.
#[doc(hidden)]
pub trait PurgePort: Send + Sync {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;
}

/// Finalizes claimed items (complete/fail/retry/release/rearm), atomically validating the lease
/// before committing: an **operator-fenced** lease is rejected with `EngineError::StaleLease` and the
/// Finalize command is NOT appended (no log/projection divergence; the fencing check is pre-commit).
/// Batch is all-or-nothing in this launch slice: any fenced item fails the whole call (per-item
/// results are a later refinement).
#[doc(hidden)]
pub trait FinalizePort: Send + Sync {
    /// `expected_epoch`: the owner's cached acquire-time fence epoch (ADR-009 / TD-003). `Some(e)` fences
    /// the commit (a superseded owner → `EpochFenced`, nothing appended); `None` = degenerate sole-owner.
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

#[doc(hidden)]
pub trait CohortFinalizePort: Send + Sync {
    fn finalize_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        kind: FinalizeKind,
        not_before: Option<UtcTimestamp>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let _ = (shard, target, kind, not_before, now, expected_epoch);
        std::future::ready(Err(EngineError::Unavailable))
    }
}

// ---------------------------------------------------------------------------
// Authoritative vectorized claimed-work commit (Snorri StateStore boundary, ADR-009 / epic
// pqueue-2201fd37)
// ---------------------------------------------------------------------------

/// A lease-token-bearing reference to a claimed item, validated INSIDE the commit boundary. Public
/// finalization no longer keys on item id alone: the presented `lease_token` must equal the stored token,
/// the lease must be unexpired (half-open: valid through `lease_expires_at`), and `item_version` must
/// equal the stored version (the optimistic state fence).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClaimRef {
    pub item_id: ItemId,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    pub item_version: u64,
}

/// A caller-supplied OPAQUE instance/state fence advanced or validated INSIDE the commit boundary (Snorri
/// authoritative-commit boundary, ADR-009 / epic pqueue-2201fd37). `instance_key` is opaque bytes pqueue
/// never interprets (e.g. a workflow instance key). The commit accepts the entry only if the queue's stored
/// fence for `instance_key` equals `expected` (an `instance_key` never advanced reads as `0` — the unset
/// convention), and `next > expected` (strictly monotonic). On accept the stored fence advances to `next`
/// ATOMICALLY in the same durable boundary as the side-record writes + input finalize; on a stale `expected`
/// the entry is rejected `Conflict` and NOTHING is written; on `next <= expected` it is rejected `Invalid`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstanceFence {
    #[serde(default)]
    pub instance_key: Vec<u8>,
    #[serde(default)]
    pub expected: u64,
    #[serde(default)]
    pub next: u64,
}

/// Validate a caller-supplied [`InstanceFence`] against the queue's currently-stored fence (`0` when the
/// `instance_key` has never advanced — the unset convention). Shared by every commit backend so the
/// accept/reject decision is identical regardless of where the fence is physically stored: `next <= expected`
/// → `Invalid` (non-monotonic, a structural request error, checked first); stored `!= expected` → `Conflict`
/// (the optimistic state fence). Mutates nothing.
pub fn validate_instance_fence(stored: u64, fence: &InstanceFence) -> EngineResult<()> {
    if fence.next <= fence.expected {
        return Err(EngineError::Invalid("instance fence is not monotonic"));
    }
    if stored != fence.expected {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

/// One entry of a vectorized transition commit: validate `claim_ref`, write opaque non-work `side_records`,
/// enqueue ordinary `lifecycle_items` (dispatchable outbox/await/timer work), and finalize the input claim
/// with `finalize`. Each entry's writes commit atomically; per-entry outcomes are independent.
///
/// `Serialize` (not `Deserialize`, since [`PushSpec`] is serialize-only) so a backend can fingerprint the
/// whole commit body for request-id idempotency.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CommitTransitionEntry {
    pub claim_ref: ClaimRef,
    pub finalize: FinalizeKind,
    pub side_records: Vec<SideRecord>,
    pub lifecycle_items: Vec<PushSpec>,
    /// Optional caller-supplied instance/state fence advanced/validated atomically with this entry (C6).
    /// `#[serde(default)]` so existing serialized commit bodies/definitions don't churn their fingerprint.
    #[serde(default)]
    pub instance_fence: Option<InstanceFence>,
}

/// A vectorized claimed-work commit request. `request_id` drives retained replay/conflict/expired
/// idempotency over the WHOLE body (TD-007 §4); `entries` are applied independently with per-entry outcomes.
#[derive(Debug, Clone)]
pub struct CommitTransition {
    pub request_id: Option<RequestId>,
    pub entries: Vec<CommitTransitionEntry>,
}

/// The per-entry result of a [`CommitTransitionPort::commit_transition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEntryOutcome {
    /// The entry validated and committed atomically. `lifecycle_item_ids` are the server-assigned ids of the
    /// entry's newly enqueued dispatchable items, in order (empty when the entry enqueued none).
    Committed { lifecycle_item_ids: Vec<ItemId> },
    /// The entry's `claim_ref` (or a lifecycle write) was rejected; NOTHING was mutated for this entry.
    Rejected(EngineError),
}

/// The authoritative vectorized claimed-work commit (Snorri StateStore boundary). One durable, recoverable
/// transition boundary per entry: lease-token + version-fence validation, opaque non-work side-record
/// writes, ordinary lifecycle enqueues, and input finalization — all atomic per entry, fenced by
/// `expected_epoch` like the other write ports. The default impl returns
/// [`EngineError::Unavailable`](crate::EngineError::Unavailable) so non-atomic / eventual-apply backends
/// (which cannot offer one atomic transition boundary) reject the operation rather than silently splitting it.
#[doc(hidden)]
pub trait CommitTransitionPort: Send + Sync {
    fn commit_transition(
        &self,
        _shard: &QueueKey,
        _transition: CommitTransition,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Capability descriptors for the authoritative vectorized claimed-work commit (Snorri StateStore boundary,
/// epic pqueue-2201fd37 acceptance, ADR-009). A consumer (Snorri) reads these BEFORE activation and rejects a
/// backend that does not advertise the guarantees it needs — every bool defaults to `false` (the safe default
/// for an eventual-apply backend that cannot offer one atomic transition boundary). Memory + sqlite-relational
/// advertise the capabilities they actually implement; objectlog/postgres keep the all-false default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCapabilities {
    /// Each commit entry's writes (side records + instance fence + lifecycle + finalize) commit atomically.
    pub atomic_transition_commit: bool,
    /// A single call commits a VECTOR of independent entries with per-entry outcomes.
    pub vectorized_commit: bool,
    /// The claim reference's lease token + lease expiry are validated inside the commit boundary.
    pub lease_validation: bool,
    /// Caller `request_id`s have retained replay/conflict/expired semantics over the whole commit body.
    pub retained_commit_idempotency: bool,
    /// Opaque non-work side records that are NOT claimable/peekable ordinary work.
    pub non_work_side_records: bool,
    /// Recovery/explain reads reconstruct the committed transition (request id, instance fence, consumed
    /// input id, side-record keys, lifecycle ids, per-entry status) from authoritative durable state.
    pub authoritative_recovery_reads: bool,
    /// Delayed/timer lifecycle items (awaits/due timers) are supported as ordinary lifecycle work.
    pub delayed_awaits_timers: bool,
    /// The durability class of the commit boundary (the clear durability boundary Snorri keys off).
    pub durability_class: DurabilityClass,
    /// A short human-readable note on the consistency boundary (e.g. "atomic append+apply under one lock").
    pub consistency: &'static str,
}

impl Default for CommitCapabilities {
    /// The safe all-false default: a backend that has not opted in advertises NO commit guarantees, so Snorri
    /// rejects it before activation. `durability_class` defaults to the weakest (`EventualApply`).
    fn default() -> Self {
        Self {
            atomic_transition_commit: false,
            vectorized_commit: false,
            lease_validation: false,
            retained_commit_idempotency: false,
            non_work_side_records: false,
            authoritative_recovery_reads: false,
            delayed_awaits_timers: false,
            durability_class: DurabilityClass::EventualApply,
            consistency: "no authoritative commit boundary",
        }
    }
}

/// Per-entry commit status surfaced by a recovery/explain read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEntryStatus {
    /// The entry validated and committed atomically.
    Committed,
    /// The entry was rejected; nothing was mutated for it. Carries the structured rejection.
    Rejected(EngineError),
}

/// One entry's reconstructed transition record (epic pqueue-2201fd37 acceptance #5). Built from the retained
/// commit idempotency record plus current durable state, so committed state/audit side records are provably
/// recoverable after input finalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRecovery {
    /// The input event id this entry consumed/finalized.
    pub consumed_input_id: ItemId,
    /// The advanced instance/state fence, if the entry carried one: `(instance_key, fence_after_advance)`.
    pub instance: Option<(Vec<u8>, u64)>,
    /// The opaque non-work side-record keys this entry wrote (empty when it wrote none).
    pub side_record_keys: Vec<Vec<u8>>,
    /// The server-assigned ids of the entry's dispatchable lifecycle items (empty when it enqueued none).
    pub lifecycle_item_ids: Vec<ItemId>,
    /// The per-entry commit status.
    pub status: CommitEntryStatus,
}

/// The reconstructed record of a vectorized claimed-work commit, addressed by its `request_id`
/// (epic pqueue-2201fd37 acceptance #5). Proves the committed transition is recoverable for retry/replay/audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecovery {
    pub request_id: RequestId,
    pub entries: Vec<EntryRecovery>,
}

/// Recovery/explain reads for the authoritative commit boundary (epic pqueue-2201fd37 acceptance #5). The
/// default impl returns [`EngineError::Unavailable`](crate::EngineError::Unavailable) so backends without an
/// authoritative commit boundary expose no (misleading) recovery surface.
#[doc(hidden)]
pub trait RecoveryReadPort: Send + Sync {
    /// Reconstruct the committed transition addressed by `request_id` from the retained commit idempotency
    /// record (plus current durable state). `Ok(None)` when no such record is retained (never committed under
    /// that id, or its retention window has elapsed).
    fn explain_commit(
        &self,
        _shard: &QueueKey,
        _request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Read an opaque non-work side record by key (recovery/audit read). `Ok(None)` if unwritten. Side records
    /// are disjoint from work items, so this never reflects claimable work and survives input finalization.
    fn side_record(
        &self,
        _shard: &QueueKey,
        _key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// In-place merge of a **live** item's hot-storage `fields`/`payload` — the write half of the
/// `LiveItemView` map (FAC-1, ADR-009). Pre-validated like finalize/renew: an absent / terminal /
/// superseded id rejects and nothing is appended; an `expected_item_version` mismatch rejects with
/// `EngineError::Conflict` (optimistic concurrency for the rolling-update case). Legal while the item is
/// Pending OR Leased; touches neither lifecycle state nor the lease. Bumps and returns the new
/// `item_version`. Atomic class only; on eventual-apply the engine returns `EngineError::Unavailable`.
#[doc(hidden)]
pub trait UpdateFieldsPort: Send + Sync {
    /// `expected_epoch`: the owner's cached acquire-time fence epoch — `Some(e)` fences the commit
    /// (superseded owner → `EpochFenced`, nothing appended); `None` is the sole-owner path.
    #[allow(clippy::too_many_arguments)]
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: crate::PayloadUpdate,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;
}

/// Reschedule a **live** item's `priority`/`not_before` after push (BQ pqueue-7a96f929) — the operator/
/// owner-runtime "change when/where this item is delivered" seam, distinct from the [`UpdateFieldsPort`]
/// field/payload merge. Pre-validated exactly like [`UpdateFieldsPort::update_fields`]: an absent / terminal
/// / superseded id rejects and nothing is appended, and an `expected_item_version` mismatch rejects with
/// `EngineError::Conflict`. Legal while the item is Pending OR Leased; a priority change re-keys the item in
/// the eligibility order and a `not_before` change re-gates its eligibility. Bumps and returns the new
/// `item_version`. The default impl returns [`EngineError::Unavailable`] so a backend that has not wired
/// reschedule (the eventual-apply object-log family, the relational family) refuses rather than silently
/// dropping the change.
#[doc(hidden)]
pub trait ReschedulePort: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn reschedule(
        &self,
        _shard: &QueueKey,
        _item_id: ItemId,
        _set_priority: crate::ScheduleUpdate<PriorityValue>,
        _set_not_before: crate::ScheduleUpdate<UtcTimestamp>,
        _expected_item_version: Option<u64>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

/// Reclaims **this queue's** leases that expired strictly before `now` (Leased → Pending), appending one
/// `LeaseExpired` command fenced by `expected_epoch`, and returns the reclaimed ids (FAC-2). Unlike the
/// global background [`ReclaimDriver::tick`], this is per-queue and fenced, so an owner-runtime sweeps
/// only the queue it owns under its own epoch — the host-driven "reclaim before claim" seam. `limit` caps
/// the batch (`None` = all expired). Idempotent: a second call with nothing newly expired returns empty.
#[doc(hidden)]
pub trait ReclaimPort: Send + Sync {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;
}

// ---------------------------------------------------------------------------
// Active-scope discovery (BQ-14e)
// ---------------------------------------------------------------------------

/// Operator discovery of a queue's **active scopes** — the groups that currently hold eligible work,
/// summarized for ranking (`DiscoverActiveScopes`, API-001 / TD-002 §Discovery). A read-only rollup over
/// the per-group summary projection (`pqueue_group_summary`): each group with `oldest_eligible_at` set
/// becomes one source [`ActiveScope`] (age from `now`, eligible count; at-risk is `None` while its
/// derivation is deferred), then [`project_scopes`](crate::project_scopes) collapses to the requested
/// granularity (per-group detail, or a single queue rollup). The returned list is ranked **owner-local,
/// oldest-first** (most-starved scope first; deterministic group-key tiebreak) — the queue has one owner
/// (ADR-008), so this ranking is authoritative for the queue without cross-owner merge.
///
/// LAYERING: this port performs the granularity projection (incl. the per-queue rollup) and the owner-local
/// sort for ITS ONE queue. A tenant-wide adapter therefore CONCATENATES these per-queue results and
/// re-ranks — it must NOT re-run [`project_scopes`](crate::project_scopes) at `Queue` granularity (the rows
/// are already one-per-queue; a second rollup is a no-op but the contract is "roll up once, here"). The
/// adapter still owns wire concerns the port does not: `tenant_id`/`as_of` stamping, `max_results`
/// truncation, and any `queue_id`/`group_key` filtering.
///
/// PAUSE: discovery reports INTRINSIC eligibility and does not short-circuit on a paused queue (it shows
/// pause-induced buildup, mirroring the pause-agnostic summary) — a deliberate divergence from the claim
/// path. KNOWN LIMITATION: read-only discovery cannot refresh groups made eligible by pure time passage, so
/// it can under-report time-triggered starvation until a mutation or background due-sweep refreshes them.
///
/// RELATIONAL-ONLY: the in-memory log-replay family maintains no per-group summary, so it does not
/// implement this port (a relational-class feature, kept out of the shared core suite — parity preserved).
pub trait DiscoveryPort: Send + Sync {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: crate::active_scope::DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::active_scope::ActiveScope>>> + Send;
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

    /// Acquire the queue at a NEW, strictly-greater `assignment_epoch` and durably record it (TD-003
    /// Single Authoritative Fencing Rule, step 1: "durable fence before use"). Returns the new epoch. This
    /// is the ownership-handoff primitive: after it commits, the previous epoch's writers are fenced at
    /// their next [`LogWriter::append`] (step 2), before any new-epoch segment exists. `assignment_epoch`
    /// MUST increase strictly and MUST NOT decrease or repeat for a queue (TD-003 epoch monotonicity).
    /// NOTE (BQ-21/BQ-23 binding): this is the storage backend's durable epoch. Some control-plane
    /// implementations, notably postgres-native, bind their acquire transaction directly to this value and
    /// make `acquire_epoch` a fallback only for control planes that cannot update the storage fence
    /// atomically. Callers should stamp the acquired owner's cached epoch on every data-plane write.
    fn acquire_epoch(
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
