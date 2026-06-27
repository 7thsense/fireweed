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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, OwnerId, PriorityValue, QueueDefinition,
    UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    ClaimPort, ClaimRequest, Clock, ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort,
    OwnedSession, OwnershipOutcome, ProjectionRead, PurgePort, PushPort, PushSpec, QueueControlPlane,
    QueueKey, ReassignLeasePort, RenewLeasePort, UpsertPort, acquire_and_fence,
};
// Re-exported so library callers name the engine's structured error + outcome/view types directly.
pub use pqueue_engine::{
    ClaimCompatibility, ClaimedItem, CreateQueueOutcome, EngineError, EngineResult, GroupBatching,
    ItemView, LiveItemView, QueueMetrics, UpsertOutcome,
};

/// The capabilities the library facade composes over (the worker + control-plane ports).
pub trait LibBackend:
    PushPort
    + ClaimPort
    + UpsertPort
    + FinalizePort
    + RenewLeasePort
    + ReassignLeasePort
    + PurgePort
    + ProjectionRead
    + ControlPlaneStore
    + Send
    + Sync
{
}
impl<T> LibBackend for T where
    T: PushPort
        + ClaimPort
        + UpsertPort
        + FinalizePort
        + RenewLeasePort
        + ReassignLeasePort
        + PurgePort
        + ProjectionRead
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
    Retry,
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

/// An item to enqueue. The id and dedup key are server-assigned for [`Pqueue::push`]; for
/// [`Pqueue::upsert`] the caller supplies the dedup `client_item_key`.
#[derive(Debug, Clone, Default)]
pub struct NewItem {
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    /// Declared cohort size (BQ-14c) — see [`ClaimCompatibility`]/`whole_cohort`. `None` for non-cohort items.
    pub cohort_size: Option<u64>,
    /// Gate keys this item carries (BQ-14d). A blocked gate key makes the item ineligible. Empty = un-gated.
    pub gate_keys: Vec<String>,
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
    /// A **sole-owner** handle (the common embedded case): no control plane, never fences. Behaviour is
    /// identical to pre-coordination pqueue.
    pub fn new(backend: Arc<B>, clock: Arc<dyn Clock>) -> Self {
        Self {
            backend,
            clock,
            ids: AtomicU64::new(0),
            coordination: Coordination::Sole,
        }
    }

    /// A **coordinated owner** over a shared control plane (ADR-009 / TD-003). Every queue-addressed op
    /// resolves ownership and operates under an acquired, epoch-fenced session, so when multiple instances
    /// share one durable backend a superseded instance is rejected `EpochFenced` at commit. The owner is
    /// runtime-refused on a backend without an atomic acquire→fence epoch in a later step (B5/OD-2).
    pub fn with_control_plane(
        backend: Arc<B>,
        clock: Arc<dyn Clock>,
        owner_id: OwnerId,
        control_plane: Arc<dyn QueueControlPlane>,
    ) -> Self {
        Self {
            backend,
            clock,
            ids: AtomicU64::new(0),
            coordination: Coordination::Owner {
                owner_id,
                control_plane,
                sessions: Mutex::new(HashMap::new()),
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
        if res.active_owner.as_ref().is_some_and(|active| active != owner_id) {
            return Err(EngineError::Forbidden("queue owned by another live owner"));
        }
        // Target-affinity (ADR-009 / TD-003): only the rendezvous `target_owner` acquires an unowned/expired
        // queue, so two instances never ping-pong a queue's epoch. A non-target surfaces owned-elsewhere.
        if res.target_owner.as_ref() != Some(owner_id) {
            return Err(EngineError::Forbidden("queue targets another owner"));
        }
        match acquire_and_fence(control_plane.as_ref(), self.backend.as_ref(), queue, owner_id, now)
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
            if control_plane
                .renew_queue_lease(&queue, owner_id, lease_epoch, now)
                .is_err()
            {
                // Superseded (or epoch-stale): drop the stale session so the next op re-resolves.
                self.invalidate_session(&queue);
            }
        }
        Ok(())
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

    /// Enqueue a batch of new items in one command (append). Returns the server-assigned ids in order.
    pub async fn push_batch(
        &self,
        queue: &QueueKey,
        items: Vec<NewItem>,
    ) -> EngineResult<Vec<ItemId>> {
        let specs: Vec<PushSpec> = items
            .into_iter()
            .map(|it| PushSpec {
                client_item_key: None,
                priority: it.priority,
                not_before: it.not_before,
                group_key: it.group_key,
                payload: it.payload,
                fields: it.fields,
                cohort_size: it.cohort_size,
                gate_keys: it.gate_keys,
            })
            .collect();
        let epoch = self.session_epoch(queue).await?;
        let r = self.backend.push(queue, specs, self.clock.now(), epoch).await;
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
    /// [`claim`](Self::claim)); group/cohort selection units are honored once their backend selection
    /// lands (BQ-14b/c) — until then a non-item unit is refused with the structured `Unavailable`.
    pub async fn claim_with(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
        compatibility: ClaimCompatibility,
    ) -> EngineResult<Vec<ClaimedItem>> {
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
        Ok(self.note(queue, r)?.items)
    }

    /// Complete (ack) the given leased items. All-or-nothing (a fenced/superseded/non-leased id rejects
    /// the batch with the structured error, committing nothing).
    pub async fn ack(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Complete).await
    }

    /// Return leased items to the queue: `Retry` or `Release`.
    pub async fn nack(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        how: Nack,
    ) -> EngineResult<()> {
        let kind = match how {
            Nack::Retry => FinalizeKind::Retry,
            Nack::Release => FinalizeKind::Release,
        };
        self.finalize(queue, ids, kind).await
    }

    async fn finalize(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
        kind: FinalizeKind,
    ) -> EngineResult<()> {
        let outcomes: Vec<FinalizeOutcome> = ids
            .into_iter()
            .map(|item_id| FinalizeOutcome { item_id, kind })
            .collect();
        let epoch = self.session_epoch(queue).await?;
        let r = self
            .backend
            .finalize(queue, outcomes, self.clock.now(), epoch)
            .await;
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

    /// Dead-letter (terminal `fail`) the given leased items.
    pub async fn fail(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Fail).await
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

    /// Re-arm a recurring item: complete this delivery and re-arm it for its next occurrence, RESETTING
    /// `attempt_count` to 0. Maps to `Finalize{Rearm}`.
    pub async fn rearm(
        &self,
        queue: &QueueKey,
        ids: impl IntoIterator<Item = ItemId>,
    ) -> EngineResult<()> {
        self.finalize(queue, ids, FinalizeKind::Rearm).await
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
