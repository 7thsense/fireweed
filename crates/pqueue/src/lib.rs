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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition, UtcTimestamp,
    WorkerId,
};
use pqueue_engine::{
    ClaimPort, ClaimRequest, Clock, ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort,
    ProjectionRead, PurgePort, PushPort, PushSpec, QueueKey, ReassignLeasePort, RenewLeasePort,
    ShardId, ShardKey, UpsertPort,
};
// Re-exported so library callers name the engine's structured error + outcome/view types directly.
pub use pqueue_engine::{
    ClaimedItem, CreateQueueOutcome, EngineError, EngineResult, ItemView, QueueMetrics,
    UpsertOutcome,
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
    let total = ts.seconds as i128 * 1_000_000_000
        + ts.nanoseconds as i128
        + millis as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

fn shard_of(queue: &QueueKey) -> ShardKey {
    ShardKey::new(queue.tenant_id.clone(), queue.queue_id.clone(), ShardId::ZERO)
}

/// How a `nack` returns an in-flight item: back to the queue for another attempt (`Retry`) or released
/// to a fresh delivery without charging the failure differently (`Release`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nack {
    Retry,
    Release,
}

/// An item to enqueue. The id and dedup key are server-assigned for [`Pqueue::push`]; for
/// [`Pqueue::upsert`] the caller supplies the dedup `client_item_key`.
#[derive(Debug, Clone, Default)]
pub struct NewItem {
    pub priority: Option<PriorityValue>,
    pub group_key: Option<GroupKey>,
    pub not_before: Option<UtcTimestamp>,
    pub payload: Option<Bytes>,
}

/// The ergonomic library handle. Holds an injected backend + clock; generates ids/lease tokens.
pub struct Pqueue<B> {
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    ids: AtomicU64,
}

impl<B: LibBackend> Pqueue<B> {
    pub fn new(backend: Arc<B>, clock: Arc<dyn Clock>) -> Self {
        Self {
            backend,
            clock,
            ids: AtomicU64::new(0),
        }
    }

    fn next(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome> {
        self.backend.create_queue(definition).await
    }

    /// Enqueue one new item (append). Routes through `PushPort`, so the backend assigns a unique,
    /// restart-safe id and commits through its divergence-safe UoW. Returns the server-assigned id.
    pub async fn push(&self, queue: &QueueKey, item: NewItem) -> EngineResult<ItemId> {
        let ids = self.push_batch(queue, vec![item]).await?;
        Ok(ids.into_iter().next().expect("one id per pushed item"))
    }

    /// Enqueue a batch of new items in one command (append). Returns the server-assigned ids in order.
    pub async fn push_batch(&self, queue: &QueueKey, items: Vec<NewItem>) -> EngineResult<Vec<ItemId>> {
        let specs: Vec<PushSpec> = items
            .into_iter()
            .map(|it| PushSpec {
                client_item_key: None,
                priority: it.priority,
                not_before: it.not_before,
                group_key: it.group_key,
                payload: it.payload,
            })
            .collect();
        self.backend
            .push(&shard_of(queue), specs, self.clock.now())
            .await
    }

    /// Upsert on a caller-supplied `client_item_key` (Invariant 2). Replaces a pending item with the
    /// same key; refused (`Unavailable`) on the eventual-apply class.
    pub async fn upsert(
        &self,
        queue: &QueueKey,
        client_item_key: ClientItemKey,
        item: NewItem,
    ) -> EngineResult<UpsertOutcome> {
        self.backend
            .replace_if_pending(
                &shard_of(queue),
                &client_item_key,
                item.priority,
                item.group_key,
                item.not_before,
                item.payload,
                self.clock.now(),
            )
            .await
    }

    /// Claim up to `max` eligible items in priority order, leasing them for `lease_ms` from now.
    pub async fn claim(
        &self,
        queue: &QueueKey,
        max: usize,
        lease_ms: u64,
    ) -> EngineResult<Vec<ClaimedItem>> {
        let now = self.clock.now();
        let n = self.next();
        let req = ClaimRequest {
            shard: shard_of(queue),
            worker_id: WorkerId::new("lib").expect("w"),
            max_items: max,
            lease_token: LeaseToken::new(format!("libL{n}")).expect("lease"),
            lease_expires_at: add_millis(now, lease_ms),
            now,
        };
        Ok(self.backend.claim(req).await?.items)
    }

    /// Complete (ack) the given leased items. All-or-nothing (a fenced/superseded/non-leased id rejects
    /// the batch with the structured error, committing nothing).
    pub async fn ack(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>) -> EngineResult<()> {
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
        self.backend
            .finalize(&shard_of(queue), outcomes, self.clock.now())
            .await
    }

    /// Non-destructive priority-ordered view of eligible items.
    pub async fn peek(&self, queue: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        self.backend.peek(&shard_of(queue), limit).await
    }

    /// Dead-letter (terminal `fail`) the given leased items.
    pub async fn fail(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>) -> EngineResult<()> {
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
        let now = self.clock.now();
        let ids: Vec<ItemId> = ids.into_iter().collect();
        self.backend
            .renew(&shard_of(queue), ids, add_millis(now, lease_ms), now)
            .await
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
        let now = self.clock.now();
        let n = self.next();
        let token = LeaseToken::new(format!("libL{n}")).expect("lease");
        let ids: Vec<ItemId> = ids.into_iter().collect();
        self.backend
            .reassign(&shard_of(queue), ids, token, add_millis(now, lease_ms), now)
            .await
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
        let ids: Vec<ItemId> = ids.into_iter().collect();
        self.backend
            .purge(&shard_of(queue), ids, force, self.clock.now())
            .await
    }

    /// Rich view of specific in-flight (leased) items in the claimed-item shape (the read behind RESP
    /// `XCLAIM`'s reply). Ids that are absent or not currently leased are omitted.
    pub async fn claimed(&self, queue: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        self.backend.claimed_view(&shard_of(queue), ids).await
    }
}
