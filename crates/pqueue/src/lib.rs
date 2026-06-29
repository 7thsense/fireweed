#![forbid(unsafe_code)]
//! # pqueue
//!
//! The ergonomic Rust **library interface** to the engine — one of the two faces of pqueue (the other
//! is the RESP/Redis-Streams wire front). It is a thin composition over the engine ports: a concrete
//! backend (memory / sqlite / objectlog / postgres) and a [`Clock`] are injected; this crate adds
//! singular, ergonomic verbs over them: `create_queue` / `push` / `push_batch` / `upsert` / `claim` /
//! `ack` / `nack` / `fail` / `renew` / `reassign` / `rearm` / `purge` / `peek` / `claimed` / `metrics` —
//! the full worker + operator surface, each composing a single pre-validating engine port.
//!
//! Dependency direction is hexagonal: this depends only on the domain (`pqueue-engine` + `pqueue-core`),
//! never on a concrete backend (a backend is passed in). Errors are the engine's structured
//! [`EngineError`]; nothing is stringly-typed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// Internal-only types (not named in the public API surface).
use pqueue_core::WorkerId;
use pqueue_engine::{
    Backend, ClaimPort, ClaimRequest, CommitEntryOutcome, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, FinalizeOutcome, FinalizePort, IndexQueryPort,
    LeaseState, OwnedSession, OwnershipOutcome, ProjectionRead, PurgePort, PushPort, PushSpec,
    QueueControlPlane, ReassignLeasePort, ReclaimPort, RecoveryReadPort, RenewLeasePort,
    ReschedulePort, SetGatesCommand, SetGatesPort, UpdateFieldsPort, UpsertPort, acquire_and_fence,
};

// ---------------------------------------------------------------------------
// PUBLIC DEPENDENCY SURFACE (ADR-009): a consumer depends on `pqueue` ALONE and can name every type its
// calls need — no direct dependency on `pqueue-core` / `pqueue-engine` required. Everything that appears in
// a public `Pqueue` signature is re-exported here.
// ---------------------------------------------------------------------------
pub use bytes::Bytes;
pub use pqueue_core::{
    ClientItemKey, CohortId, CohortOnIncomplete, CohortPolicy, CreateQueue, CreateQueueError,
    CreateQueueErrorKind, DecimalValue, EligibilityPolicy, GateKeyPolicy, GroupKey,
    IdentifierError, IndexSpec, ItemId, LeaseToken, Metadata, MetadataValue, OrderingMode, OwnerId,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueCreationPolicy, QueueDefinition, QueueId, RecurrenceMode, RecurrencePolicy, RequestId,
    RetryPolicy, TenantId, TimestampError, UtcTimestamp,
};
pub use pqueue_engine::{
    ClaimCompatibility, ClaimRef, Claimed, ClaimedItem, Clock, CommitCapabilities,
    CommitEntryStatus, CommitRecovery, ControlPlaneConfig, CreateQueueOutcome, EngineError,
    EngineResult, EntryRecovery, FinalizeKind, GroupBatching, IndexHit, InstanceFence, ItemView,
    LiveItemView, PayloadUpdate, QueueKey, QueueMetrics, ScheduleUpdate, SideRecord, UpsertOutcome,
};

/// Wall-clock [`Clock`] for production use — pass `Arc::new(SystemClock)` to any `open_*` constructor.
/// Tests inject a controllable clock instead (e.g. `pqueue_memory::ManualClock`). Provided here so a
/// consumer depending on `pqueue` alone has a ready clock without naming `pqueue-engine`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}

/// The capabilities the library facade composes over (the worker + control-plane ports). This is an
/// INTERNAL composition bound, not a consumer-facing trait: a backend satisfies it automatically (blanket
/// impl over the engine ports) and a consumer never names or implements it. Hidden from the public docs.
#[doc(hidden)]
pub trait LibBackend:
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
    + IndexQueryPort
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
        + IndexQueryPort
        + ControlPlaneStore
        + Send
        + Sync
{
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

/// How a `nack` returns an in-flight item: back to the queue for another attempt (`Retry`) or released
/// to a fresh delivery without charging the failure differently (`Release`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nack {
    /// Return to Pending for re-claim. `not_before` is an optional **queue-native retry backoff**: the item
    /// stays ineligible until that absolute timestamp. `None` re-eligibles it immediately. (Use
    /// [`Pqueue::nack_retry_after`] for a relative delay.)
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

/// An item to enqueue. For [`Pqueue::push`], `client_item_key` is optional and defaults to the
/// server-assigned id when omitted; for [`Pqueue::upsert`], the caller supplies the dedup key as the
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

/// The per-entry result of a [`Pqueue::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryOutcome {
    /// The entry validated and committed atomically. `lifecycle_item_ids` are the server-assigned ids of the
    /// entry's newly enqueued dispatchable items, in order (empty when the entry enqueued none).
    Committed { lifecycle_item_ids: Vec<ItemId> },
    /// The entry's `claim_ref` (or a lifecycle write) was rejected; NOTHING was mutated for this entry.
    Rejected(EngineError),
}

/// How a [`Pqueue`] handle coordinates ownership (ADR-009 / TD-003 In-Process Library Owner-Runtime).
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
        sessions: Mutex<HashMap<QueueKey, OwnedSession>>,
        /// Queues observed `Draining` on the renew loop (TD-003 §Graceful Drain). While a queue is here the
        /// owner serves in-flight ops but refuses a NEW claim with a retryable `Unavailable` (drain split).
        draining: Mutex<HashSet<QueueKey>>,
    },
}

/// The ergonomic library handle. Holds an injected backend + clock; generates ids/lease tokens.
pub struct Pqueue<B> {
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    ids: AtomicU64,
    coordination: Coordination,
}

impl<B: LibBackend> Pqueue<B> {
    /// Low-level backend-injection constructor for a **sole-owner** handle. Hidden from the published
    /// surface (ADR-009 §4a / L6): external clients build via [`open_memory`]/[`open_sqlite`]/
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
    /// (omit it, via [`Pqueue::new`]/`open_*`, for a single-instance deployment). Every queue-addressed op
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
    /// [`Pqueue::with_control_plane`].
    #[doc(hidden)]
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
        let Coordination::Owner {
            owner_id,
            control_plane,
            sessions,
            ..
        } = &self.coordination
        else {
            return Ok(None);
        };
        if let Some(s) = sessions.lock().expect("poisoned").get(queue) {
            return Ok(Some(s.fence_epoch));
        }
        let now = self.clock.now();
        control_plane.register_owner(owner_id, now)?;
        let res = control_plane.resolve_queue_owner(queue, now)?;
        // A DIFFERENT live owner holds the queue → owned elsewhere; never contend a live lease (TD-003:
        // online handoff is begin_drain, not a contended acquire). Surface it (callers inspect via
        // `ownership`); the explicit redirect is the RESP server's `-MOVED`.
        if res
            .active_owner
            .as_ref()
            .is_some_and(|active| active != owner_id)
        {
            return Err(EngineError::Forbidden("queue owned by another live owner"));
        }
        // Target-affinity (ADR-009 / TD-003): only the rendezvous `target_owner` acquires an unowned/expired
        // queue, so two instances never ping-pong a queue's epoch. A non-target surfaces owned-elsewhere.
        if res.target_owner.as_ref() != Some(owner_id) {
            return Err(EngineError::Forbidden("queue targets another owner"));
        }
        match acquire_and_fence(
            control_plane.as_ref(),
            self.backend.as_ref(),
            queue,
            owner_id,
            now,
        )
        .await?
        {
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
            ..
        } = &self.coordination
        else {
            return Ok(Ownership::Mine { epoch: None });
        };
        let res = control_plane.resolve_queue_owner(queue, self.clock.now())?;
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
            sessions,
            draining,
        } = &self.coordination
        else {
            return Ok(());
        };
        let now = self.clock.now();
        control_plane.heartbeat(owner_id, now)?;
        let owned: Vec<(QueueKey, u64)> = sessions
            .lock()
            .expect("poisoned")
            .iter()
            .map(|(q, s)| (q.clone(), s.lease_epoch))
            .collect();
        for (queue, lease_epoch) in owned {
            match control_plane.renew_queue_lease(&queue, owner_id, lease_epoch, now) {
                Ok(lease) => {
                    // Observe drain on the renew loop (TD-003): a `Draining` lease ⇒ stop serving NEW claims
                    // for this queue (drain split); a non-draining lease clears the flag.
                    let mut d = draining.lock().expect("poisoned");
                    if lease.state == LeaseState::Draining {
                        d.insert(queue.clone());
                    } else {
                        d.remove(&queue);
                    }
                }
                // Superseded (or epoch-stale): drop the stale session so the next op re-resolves.
                Err(_) => {
                    draining.lock().expect("poisoned").remove(&queue);
                    self.invalidate_session(&queue);
                }
            }
        }
        Ok(())
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

    /// Enqueue one new item (append). Routes through `PushPort`, so the backend assigns a unique,
    /// restart-safe id and commits through its divergence-safe UoW. Returns the server-assigned id.
    pub async fn push(&self, queue: &QueueKey, item: NewItem) -> EngineResult<ItemId> {
        let ids = self.push_batch(queue, vec![item]).await?;
        Ok(ids.into_iter().next().expect("one id per pushed item"))
    }

    /// Enqueue one item under an API-001 request id. Replaying the same request body with the same
    /// `request_id` returns the original item id on backends that implement durable request replay.
    pub async fn push_with_request_id(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
        item: NewItem,
    ) -> EngineResult<ItemId> {
        let ids = self
            .push_batch_with_request_id(queue, request_id, vec![item])
            .await?;
        Ok(ids.into_iter().next().expect("one id per pushed item"))
    }

    /// Enqueue a batch of new items in one command (append). Returns the server-assigned ids in order.
    pub async fn push_batch(
        &self,
        queue: &QueueKey,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        self.push_batch_inner(queue, None, items).await
    }

    /// Enqueue a batch under an API-001 request id. Replaying the same batch body with the same request id
    /// returns the original ids in order; a different body returns `RequestIdConflict`.
    pub async fn push_batch_with_request_id(
        &self,
        queue: &QueueKey,
        request_id: RequestId,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        self.push_batch_inner(queue, Some(request_id), items).await
    }

    async fn push_batch_inner(
        &self,
        queue: &QueueKey,
        request_id: Option<RequestId>,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        let specs: Vec<PushSpec> = items.into_iter().map(new_item_to_spec).collect();
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = if let Some(request_id) = request_id {
            self.backend
                .push_with_request_id(queue, request_id, specs, now, epoch)
                .await
        } else {
            self.backend.push(queue, specs, now, epoch).await
        };
        self.note(queue, r)
    }

    /// Upsert on a caller-supplied `client_item_key` (Invariant 2). Replaces a pending item with the
    /// same key; refused (`Unavailable`) on the eventual-apply class.
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
    /// [`claim`](Self::claim)); backends that do not implement a requested non-item unit refuse it with
    /// the structured `Unavailable` rather than silently downgrading to item-level delivery.
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
        // Drain split (TD-003 §Graceful Drain): a draining owner refuses a NEW claim with a retryable
        // `Unavailable` so in-flight leases finalize before handoff; pushes/finalizes/renews continue.
        if self.is_draining(queue) {
            return Err(EngineError::Unavailable);
        }
        let expected_epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let n = self.next();
        let req = ClaimRequest {
            shard: queue.clone(),
            worker_id: WorkerId::new("lib").expect("w"),
            max_items: max,
            lease_token: LeaseToken::new(format!("libL{n}")).expect("lease"),
            lease_expires_at: add_millis(now, lease_ms),
            now,
            compatibility,
            // Sole-owner: None (never fences). Coordinated owner: the cached acquire-time fence epoch.
            expected_epoch,
        };
        let r = self.backend.claim(req).await;
        self.note(queue, r)
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

    /// Authoritative vectorized claimed-work commit (Snorri StateStore boundary, epic pqueue-2201fd37).
    /// Each [`CommitEntry`] is ONE recoverable transition: it validates a lease-token + version-fenced
    /// [`ClaimRef`], writes opaque non-work `side_records` (authoritative workflow state/audit that is NOT
    /// claimable work), enqueues `lifecycle_items` as ordinary dispatchable work (outbox/await/timer), and
    /// finalizes the input claim — atomically per entry. `request_id` gives the whole body retained
    /// replay/conflict/expired semantics, so a retried transition returns the prior outcomes without
    /// double-writing. Per-entry [`EntryOutcome`]s are independent (all-or-nothing is NOT required across
    /// entries). Backends without an atomic transition boundary reject with [`EngineError::Unavailable`].
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

    /// The backend's authoritative-commit capability descriptors (epic pqueue-2201fd37, ADR-009). A consumer
    /// (Snorri) reads these BEFORE activation and rejects a backend that does not advertise the guarantees it
    /// needs (e.g. `atomic_transition_commit`). Memory + sqlite-relational advertise the real capabilities;
    /// objectlog/postgres keep the all-false default. `queue` is accepted for signature stability — the
    /// capability set is backend-wide.
    pub fn commit_capabilities(&self, _queue: &QueueKey) -> EngineResult<CommitCapabilities> {
        Ok(self.backend.commit_capabilities())
    }

    /// Recovery/explain read for a committed transition (epic pqueue-2201fd37 acceptance #5). Reconstructs the
    /// transition addressed by `request_id` — the consumed input id, the advanced instance fence, the
    /// side-record keys, the lifecycle item ids, and per-entry status — from the retained commit idempotency
    /// record plus current durable state. `Ok(None)` when no such record is retained. Proves committed
    /// state/audit remains recoverable after the input is finalized. Backends without an authoritative commit
    /// boundary reject with [`EngineError::Unavailable`].
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
    /// `Ok(None)` if unwritten. Backends without an authoritative commit boundary reject with
    /// [`EngineError::Unavailable`].
    pub async fn side_record(&self, queue: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        let r = self.backend.side_record(queue, key).await;
        self.note(queue, r)
    }

    /// Non-destructive priority-ordered view of eligible items.
    pub async fn peek(&self, queue: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        self.backend.peek(queue, limit).await
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
    /// Pure read (no epoch/fence). `EngineError::Invalid` if `index` is not a unique index on this queue;
    /// `EngineError::Unavailable` on a relational backend (Phase 2).
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
    pub async fn query_index(
        &self,
        queue: &QueueKey,
        index: &str,
        key: Vec<Vec<u8>>,
    ) -> EngineResult<Vec<IndexHit>> {
        self.backend.index_lookup(queue, index, &key).await
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
    /// lease token. Pre-validated like [`Pqueue::renew`].
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
    /// pqueue instead of a side shadow store. Legal while the item is Pending OR Leased; touches neither
    /// lifecycle state nor the lease. `field_ops`: `Some(bytes)` sets/overwrites a key, `None` removes it.
    /// `payload`: [`PayloadUpdate::Keep`] leaves the body, `Set(_)` replaces (`Set(None)` clears).
    /// `expected_item_version`: optional CAS — a mismatch rejects with [`EngineError::Conflict`] and commits
    /// nothing (for rolling concurrent updates). Bumps and returns the new `item_version`. Fenced by the
    /// owner's epoch. Atomic class only: an eventual-apply backend rejects with [`EngineError::Unavailable`].
    pub async fn update_fields(
        &self,
        queue: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        expected_item_version: Option<u64>,
    ) -> EngineResult<u64> {
        let epoch = self.session_epoch(queue).await?;
        let now = self.clock.now();
        let r = self
            .backend
            .update_fields(
                queue,
                item_id,
                field_ops,
                payload,
                expected_item_version,
                now,
                epoch,
            )
            .await;
        self.note(queue, r)
    }

    /// Reschedule a **live** item's `priority` and/or `not_before` after push (BQ pqueue-7a96f929) — the
    /// "change when/where this item is delivered" verb, distinct from [`Pqueue::update_fields`] (which merges
    /// hot-storage fields/payload). [`ScheduleUpdate::Keep`] leaves a dimension unchanged; `Set(Some(v))`
    /// sets it; `Set(None)` clears it (clearing `not_before` makes the item immediately eligible; clearing
    /// `priority` drops it to the unpriced FIFO tail). A priority change re-keys the item in the eligibility
    /// order; a `not_before` change re-gates its eligibility (so a deferred item leaves the claimable set
    /// until its new time). Legal while the item is Pending OR Leased; pre-validated like `update_fields`
    /// (absent/terminal/superseded id → reject; `expected_item_version` mismatch → [`EngineError::Conflict`]),
    /// fenced by the owner's epoch. Bumps and returns the new `item_version`. Atomic class only — the
    /// eventual-apply object-log family and the relational family return [`EngineError::Unavailable`].
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
    /// SCOPE (TD-002 §gate): gates are a RELATIONAL-mode feature. A gate-capable backend (the relational
    /// family) enforces this; the log-replay / in-memory family stores no gate state and rejects it with
    /// [`EngineError::Unavailable`] (carrying a gate key on a log-replay queue is already rejected at push).
    /// Fenced by the owner's epoch.
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

    /// Re-arm a recurring item: complete this delivery and re-arm it for its next occurrence, RESETTING
    /// `attempt_count` to 0. Maps to `Finalize{Rearm}` with no new `not_before` (re-eligible immediately).
    /// For a recurring item with an idle interval between occurrences use [`Pqueue::rearm_at`].
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

    /// [`Pqueue::rearm_at`] with a **relative** interval: re-arm for `delay_ms` from now (the recurrence
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
}

// ---------------------------------------------------------------------------
// Public constructors — the blessed way to build a Pqueue WITHOUT naming a backend (ADR-009 §4a / B3).
// The concrete backend is built internally and erased behind `impl LibBackend`, so a client of the
// published crate never holds a port-bearing handle. Reaching a raw port requires deliberately depending
// on an internal crate (strong-by-default, not absolute — OD-6).
// ---------------------------------------------------------------------------

/// Open a **sole-owner**, in-memory pqueue (atomic durability class) — the zero-setup embedded path.
/// Requires the `memory` feature (default).
#[cfg(feature = "memory")]
pub fn open_memory(clock: Arc<dyn Clock>) -> Pqueue<impl LibBackend> {
    Pqueue::new(Arc::new(pqueue_memory::MemoryBackend::new()), clock)
}

/// Open a **sole-owner**, sqlite-backed pqueue (durable log + projection rebuilt from the log) at `path`.
/// Requires the `sqlite` feature (default).
#[cfg(feature = "sqlite")]
pub fn open_sqlite(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Pqueue<impl LibBackend>> {
    Ok(Pqueue::new(
        Arc::new(pqueue_sqlite::SqliteBackend::open(path)?),
        clock,
    ))
}

/// Open a **sole-owner**, object-log pqueue (eventual-apply class) rooted at `root`. Requires the
/// `objectlog` feature (default).
#[cfg(feature = "objectlog")]
pub fn open_objectlog(
    root: impl Into<std::path::PathBuf>,
    clock: Arc<dyn Clock>,
) -> EngineResult<Pqueue<impl LibBackend>> {
    Ok(Pqueue::new(
        Arc::new(pqueue_objectlog::ObjectLogBackend::open(root)?),
        clock,
    ))
}

/// Open a **sole-owner** postgres-backed pqueue (log-replay class) at `url`. Requires the `postgres`
/// feature (opt-in). For a durable **multi-instance** deployment use [`open_postgres_coordinated`].
#[cfg(feature = "postgres")]
pub fn open_postgres(url: &str, clock: Arc<dyn Clock>) -> EngineResult<Pqueue<impl LibBackend>> {
    Ok(Pqueue::new(
        Arc::new(pqueue_postgres::PostgresBackend::connect(url)?),
        clock,
    ))
}

/// Open a **durable multi-instance** coordinated postgres pqueue: builds the postgres backend AND the
/// transactional postgres control plane (which binds the storage fence epoch, BQ-23) against `url`, and
/// returns a coordinated [`Pqueue`] for this `instance_id`. Requires the `postgres` feature. The client
/// never names a backend or control plane. (Run each process with a distinct `instance_id`.)
#[cfg(feature = "postgres")]
pub fn open_postgres_coordinated(
    url: &str,
    clock: Arc<dyn Clock>,
    instance_id: OwnerId,
    control_plane_config: pqueue_engine::ControlPlaneConfig,
) -> EngineResult<Pqueue<impl LibBackend>> {
    let backend = Arc::new(pqueue_postgres::PostgresBackend::connect(url)?);
    let control_plane: Arc<dyn QueueControlPlane> = Arc::new(
        pqueue_postgres::PostgresControlPlane::connect(url, control_plane_config)?,
    );
    Pqueue::with_control_plane(backend, clock, instance_id, control_plane)
}
