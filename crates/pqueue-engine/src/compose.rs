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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use pqueue_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByQueryRequest, ClientItemKey,
    DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, GroupKey, GroupedAggregateRequest,
    GroupedAggregateResponse, ItemId, ItemState, LeaseToken, Metadata, OrderingMode, PriorityValue,
    QueryCapabilityFlags, QueueDefinition, QueueId, RangeScanRequest, RangeScanResponse, RequestId,
    TenantId, UtcTimestamp,
};

use crate::claim_validation::{ClaimCompatibility, require_item_level_claim};
use crate::command::{
    AdvanceInstanceFenceCommand, ClaimCommand, CommandChecksum, CommandEnvelope, CommandId,
    FinalizeCommand, FinalizeOutcome, LeaseExpiredCommand, PayloadUpdate, PurgeItemsCommand,
    PushCommand, PushItem, QueueCommand, QueueCounters, ReassignLeaseCommand, RenewLeaseCommand,
    ReplacePendingCommand, RequestOutcome, ScheduleUpdate, UpdateFieldsCommand,
    WriteSideRecordsCommand, build_push_items, command_envelope_change_records,
    validate_gate_command, validate_gate_push, validate_request_replay_metadata,
};
use crate::error::{EngineError, EngineResult};
use crate::finalize_validation::validate_purge_force;
use crate::idempotency::{IdempotencyDecision, QueueIdempotencyCache};
use crate::port::{
    Backend, ClaimPort, ClaimRef, ClaimRequest, Claimed, ClaimedItem, CommandPage,
    CommitCapabilities, CommitEntryOutcome, CommitEntryStatus, CommitRecovery, CommitTransition,
    CommitTransitionPort, ControlPlaneStore, CreateQueueOutcome, EntryRecovery, FinalizePort,
    IndexHit, IndexQueryPort, ItemView, LeaseView, LiveItemView, LogRead, LogWriter,
    ProjectionRead, ProjectionSnapshot, ProjectionWriter, PurgePort, PushPort, PushSpec,
    QueueMetrics, ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeasePort,
    ReschedulePort, SnapshotRef, SnapshotStore, TickReport, UpdateFieldsPort, UpsertOutcome,
    UpsertPort, AsOfProjectionStore, HistoricalProjectionRead, validate_instance_fence,
};
use crate::schema_validation::{compile_entity_schema, validate_entity};
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
    /// The durability class the composition inherits from its log axis (TD-007 §2). The default is
    /// [`DurabilityClass::Atomic`] (the in-process/sqlite log-replay logs commit append+apply together under
    /// one lock); an eventual-apply substrate (the object log's ack-after-seal group commit) overrides this
    /// to [`DurabilityClass::EventualApply`], which the composition uses to refuse the atomic-only ports
    /// (upsert / update_fields / reschedule / commit_transition) rather than silently degrading them.
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

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

    /// The current durable command position for `shard` (thin wrapper over `high_water`).
    fn current_position(&self, shard: &QueueKey) -> EngineResult<CommandPosition> {
        self.high_water(shard)?.ok_or(EngineError::NotFound)
    }

    /// Find the newest snapshot whose position is `<= position`.
    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Option<SnapshotRef>> {
        let latest = self.latest_snapshot(shard)?;
        Ok(match latest {
            Some(snapshot)
                if snapshot.position.precedes(position) || snapshot.position == *position =>
            {
                Some(snapshot)
            }
            _ => None,
        })
    }

    /// Durable tail cursor for change-record emission. Default: no stored cursor yet.
    fn emission_cursor(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        Ok(None)
    }

    /// Persist the change-record emission cursor after a successful sink emit.
    fn set_emission_cursor(
        &mut self,
        _shard: &QueueKey,
        _position: CommandPosition,
    ) -> EngineResult<()> {
        Ok(())
    }

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef>;
    fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>>;
    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot>;

    /// Persist `definition` in the log's durable queue catalog (called from `create_queue` when a queue is
    /// first created) so a reopened composition can enumerate its queues for recovery WITHOUT a
    /// re-`create_queue`. Default: no-op — an in-process log ([`crate::MemoryLog`] analogue) or a unified
    /// relational store (whose definitions live in its projection axis) persist nothing here.
    fn persist_definition(&mut self, _definition: &QueueDefinition) -> EngineResult<()> {
        Ok(())
    }

    /// Enumerate the durable queue definitions this log persists, for recovery-on-open (ADR-012 P2). Default:
    /// empty — a reopened in-process log is a fresh process with nothing to recover.
    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(Vec::new())
    }

    // -- group-commit facet (ADR-012 P2, runtime-agnostic) -------------------
    //
    // The OPTIONAL ack-after-seal co-buffering seam (TD-004): instead of force-sealing one segment per
    // `append`, the composition buffers concurrent writes per queue and seals MANY commands into one durable
    // object (size- or latency-triggered), amortizing the per-object cost. These are `&self` (the substrate
    // is interior-mutable) so the composition can call them while holding the unit-of-work lock WITHOUT
    // `&mut self`. A log that does not opt in keeps `supports_group_commit() == false` and the composition
    // uses the synchronous `append` path unchanged — the defaults here are never reached on the OFF path.

    /// Whether this log implements the group-commit facet (the composition's ack-after-seal co-buffering
    /// write path). `false` (the default) keeps the synchronous force-seal `append` path.
    fn supports_group_commit(&self) -> bool {
        false
    }

    /// Buffer `commands` for `shard` under `expected_epoch` (TD-004 step 1). If a size trigger fires the
    /// buffered batch seals synchronously and its acked positions are returned; otherwise the commands stay
    /// buffered (NOT acked) and an empty vec is returned. `now_ms` stamps the oldest-buffered age for the
    /// latency trigger. Default: unsupported (never called on the OFF path).
    fn gc_enqueue(
        &self,
        _shard: &QueueKey,
        _commands: &[CommandEnvelope],
        _expected_epoch: u64,
        _now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        Err(EngineError::Unavailable)
    }

    /// Force-seal whatever is buffered for `shard` into one segment and ack (TD-004 step 2 forced). A stale
    /// `expected_epoch` is fenced before any object is written; empty if nothing was buffered. Default:
    /// unsupported.
    fn gc_seal(
        &self,
        _shard: &QueueKey,
        _expected_epoch: u64,
        _now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        Err(EngineError::Unavailable)
    }

    /// Seal the buffered batch for `shard` iff its oldest command has aged past the latency cap (TD-004 step 2
    /// latency trigger); acked positions, or empty if nothing was due. Default: unsupported.
    fn gc_flush_due(
        &self,
        _shard: &QueueKey,
        _expected_epoch: u64,
        _now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        Err(EngineError::Unavailable)
    }

    /// Advance the durable high-water to the last acked `position` after a seal (no monotonic re-check — the
    /// post-seal position always advances). Default: unsupported.
    fn gc_advance_high_water(
        &self,
        _shard: &QueueKey,
        _position: CommandPosition,
    ) -> EngineResult<()> {
        Err(EngineError::Unavailable)
    }

    /// The configured latency cap (ms). The externalized flusher polls each queue at `gc_max_latency_ms()/4`
    /// so a buffer below the size threshold still acks within ~one latency window. Default: `0`.
    fn gc_max_latency_ms(&self) -> u64 {
        0
    }
}

// ---------------------------------------------------------------------------
// Runtime-free seal-wait (group-commit ack-after-seal, ADR-012 P2)
// ---------------------------------------------------------------------------

/// A one-shot seal-result slot a group-commit waiter parks on until its co-buffered batch seals. Runtime-
/// free: [`SealFuture`] polls the slot and registers a [`Waker`]; [`SealSlot::complete`] fills the slot and
/// wakes the parked poller. `EngineError: Clone`, so one seal outcome fans out to every waiter on the batch.
struct SealSlot {
    result: Mutex<Option<EngineResult<()>>>,
    waker: Mutex<Option<Waker>>,
}

impl SealSlot {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        }
    }

    /// Fill the slot with the seal outcome and wake any parked poller. The result lock is held while taking
    /// the waker so a concurrent [`SealFuture::poll`] either observes the result on its next poll or has its
    /// just-registered waker woken — no lost wakeup (poll registers its waker under the same result lock).
    fn complete(&self, outcome: EngineResult<()>) {
        let mut r = self.result.lock().expect("seal slot poisoned");
        *r = Some(outcome);
        let waker = self.waker.lock().expect("seal slot poisoned").take();
        drop(r);
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// The future a write port returns. `Ready` resolves immediately (the synchronous atomic path, and the
/// group-commit ops that complete under the lock); `Seal` parks on a [`SealSlot`] until the waiter's co-
/// buffered batch seals (the ack-after-seal `push`). Works on any executor (no runtime dependency).
enum AckFuture<T> {
    Ready(Option<EngineResult<T>>),
    Seal {
        slot: Arc<SealSlot>,
        value: Option<T>,
    },
}

struct SealWait {
    slot: Arc<SealSlot>,
}

impl SealWait {
    fn new(slot: Arc<SealSlot>) -> Self {
        Self { slot }
    }
}

impl Future for SealWait {
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut r = self.slot.result.lock().expect("seal slot poisoned");
        if let Some(outcome) = r.take() {
            return Poll::Ready(outcome);
        }
        *self.slot.waker.lock().expect("seal slot poisoned") = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> AckFuture<T> {
    fn ready(result: EngineResult<T>) -> Self {
        AckFuture::Ready(Some(result))
    }

    fn seal(slot: Arc<SealSlot>, value: T) -> Self {
        AckFuture::Seal {
            slot,
            value: Some(value),
        }
    }
}

impl<T: Unpin> Future for AckFuture<T> {
    type Output = EngineResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            AckFuture::Ready(slot) => {
                Poll::Ready(slot.take().expect("AckFuture polled after completion"))
            }
            AckFuture::Seal { slot, value } => {
                let mut r = slot.result.lock().expect("seal slot poisoned");
                if let Some(outcome) = r.take() {
                    return Poll::Ready(match outcome {
                        Ok(()) => Ok(value.take().expect("AckFuture polled after completion")),
                        Err(e) => Err(e),
                    });
                }
                // Not sealed yet: register the waker WHILE holding the result lock (so `complete` cannot slip
                // a result+wake between our check and our registration), then yield.
                *slot.waker.lock().expect("seal slot poisoned") = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Per-queue group-commit coordination (ADR-012 P2): the engine-side mirror of the substrate's buffer. Held
/// under the composition's existing `std::sync::Mutex<Inner>` — no async lock. `pending` mirrors the
/// substrate's buffered envelopes 1:1 in arrival order (so a seal that drains the substrate buffer drains
/// exactly the same `pending`/`waiters` prefix); each `waiters` slot is filled when its batch seals + applies.
#[derive(Default)]
struct ShardCoord {
    /// Envelopes buffered-but-not-yet-acked, kept engine-side so `distribute` can apply them to the
    /// projection on seal (the substrate's seal returns only positions).
    pending: Vec<CommandEnvelope>,
    /// One seal slot per buffered envelope; completed (Ok/Err) when the envelope's segment seals + applies.
    waiters: Vec<Arc<SealSlot>>,
    /// The assignment epoch the buffered batch will seal under (set when the first command buffers).
    seal_epoch: u64,
    /// Claim commands that have selected candidates and are waiting for durable object-log seal. Until the
    /// seal applies them to the projection, later claims must exclude these ids to avoid duplicate leases.
    in_flight_claims: BTreeSet<ItemId>,
    /// Last item reserved by an in-flight claim in strict eligibility order. Strict queues can resume
    /// candidate selection after this key instead of rescanning and filtering the full reserved prefix.
    in_flight_claim_tail: Option<ItemId>,
}

/// `UtcTimestamp` → epoch milliseconds (the substrate's latency-trigger clock unit).
fn ts_to_ms(now: UtcTimestamp) -> i64 {
    now.seconds
        .saturating_mul(1000)
        .saturating_add((now.nanoseconds / 1_000_000) as i64)
}

// ---------------------------------------------------------------------------
// Axis 2: ProjectionStore — the materialized read model
// ---------------------------------------------------------------------------

/// The object-log's durable identity for one shard, presented to the projection during recovery-on-open so a
/// hybrid projection can cross-validate its own durably recorded lineage against the log it is about to
/// replay from (TD-004 "Async lineage validation": manifest/segment/high-water identity). The composition
/// builds this from the [`LogStore`] axis ([`LogStore::current_epoch`] + [`LogStore::high_water`]) after the
/// projection has been hydrated but BEFORE any tail replay; a projection whose recorded lineage does not
/// descend from this identity MUST fail closed rather than serve a divergent image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLineageIdentity {
    /// The queue namespace (tenant/queue) the log identity is for.
    pub shard: QueueKey,
    /// The log's current `assignment_epoch` (the highest epoch any committed manifest entry records).
    pub current_epoch: u64,
    /// The log's durable committed head, or `None` for an empty log. `sequence + 1` is the next command
    /// sequence the log will assign, i.e. the exclusive upper bound on any projection's applied prefix.
    pub high_water: Option<CommandPosition>,
}

/// Where recovery-on-open must begin replaying the durable log for one shard, once poison / backpressure
/// health has been folded in (see [`resolve_recovery_start`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryStart {
    /// Trust the projection's durably recorded high-water and replay only the object-log tail beyond it.
    FromHighWater(Option<CommandPosition>),
    /// The projection's high-water is not trustworthy (HARD async-apply backpressure); replay the whole
    /// retained log from genesis rather than skipping history. The high-water is NOT advanced past the
    /// unvalidated debt.
    FromGenesis,
}

/// Resolve where recovery replay must start for a shard, given the projection's recorded high-water and its
/// async-apply health (TD-004 §"Async apply debt, backpressure, and poison thresholds").
///
/// - **Poison ⇒ fail closed.** If `poison` is `Some`, the projection has latched an unrepairable state
///   (persistent checkpoint errors, corruption, or an unresolved replay-apply gap). Unresolved replay
///   poison MUST stop serving, so this returns `Err(Storage)` — the composition aborts recovery rather than
///   hydrating a divergent image. This is the "high-water must not advance past poison" invariant: a
///   poisoned shard never reaches the tail-replay loop that would advance state.
/// - **Hard backpressure ⇒ replay from genesis.** If the projection is under hard async-apply
///   backpressure (`hard_backpressure == true`) but not poisoned, its lagging `sqlite_high_water` MUST NOT
///   be advertised as a safe replay-skip point; recovery replays from an earlier authoritative source
///   (genesis) so no acknowledged command is skipped.
/// - **Healthy ⇒ trust the high-water.** Otherwise replay only the object-log tail beyond the recorded
///   high-water (the normal owner-local fast restart).
pub fn resolve_recovery_start(
    poison: Option<&str>,
    hard_backpressure: bool,
    high_water: Option<CommandPosition>,
) -> EngineResult<RecoveryStart> {
    if let Some(reason) = poison {
        return Err(EngineError::Storage(format!(
            "recovery refused: hybrid-async projection is poisoned and must not advance past the poison \
             point: {reason}"
        )));
    }
    if hard_backpressure {
        return Ok(RecoveryStart::FromGenesis);
    }
    Ok(RecoveryStart::FromHighWater(high_water))
}

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

    /// Apply committed commands to the live serving image. Durable projections use the same implementation
    /// by default; hybrid async projections may return after memory apply and queue durable checkpoint work.
    fn apply_live(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply(positions, commands)
    }

    /// Owned variant of [`Self::apply_live`] for group-commit paths that already own the sealed envelopes.
    /// Most projections can borrow and drop the owned values; hybrid async projections override this to move
    /// envelopes into their deferred durable-apply queue without cloning large push batches on the ack path.
    fn apply_live_owned(
        &mut self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> EngineResult<()> {
        self.apply_live(&positions, &commands)
    }

    /// Apply committed commands during recovery. Defaults to the durable apply path so restart catch-up
    /// leaves the projection's persisted high-water exactly at the replayed log prefix.
    fn apply_recovery(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply(positions, commands)
    }

    /// Drain any deferred durable projection work. Default is a no-op for synchronous projections.
    fn flush_deferred(&mut self) -> EngineResult<()> {
        Ok(())
    }

    // -- claim / orchestration reads ----------------------------------------

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>>;
    fn eligible_candidates_after(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        after: Option<ItemId>,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        let _ = after;
        self.eligible_candidates(shard, now, max)
    }
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
        entity: Option<&serde_json::Value>,
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
        entity: Option<&serde_json::Value>,
    ) -> EngineResult<()>;

    // -- commit-class (Snorri authoritative vectorized commit boundary, ADR-009 / epic pqueue-2201fd37) --
    //
    // These back the composition's [`CommitTransitionPort`] / [`RecoveryReadPort`] (the side-record write +
    // instance-fence advance themselves ride ordinary `QueueCommand`s through `apply`, so only the PRE-commit
    // reads/validation live here). The default impls are the safe eventual/relational-stub answers: a
    // projection that has not opted in advertises NO commit boundary (`supports_commit_transition() == false`),
    // so the composition refuses `commit_transition` with `Unavailable` before touching these. ADR-012 1b-ii's
    // unified relational store overrides `supports_commit_transition` + these reads with its own SQL.

    /// Whether this projection materializes the Snorri commit-class read model (side records, instance fences,
    /// lease-token/version commit validation). `false` (the default) makes the composition reject
    /// `commit_transition` with `Unavailable`; [`InMemoryProjection`] overrides it to `true`.
    fn supports_commit_transition(&self) -> bool {
        false
    }

    /// Pre-commit validation of a vectorized commit's lease-token + version-fenced `claim_ref`s
    /// ([`ProjectionData::commit_validate`] semantics). Mutates nothing. The default refuses with
    /// `Unavailable` (no commit-class read model).
    fn commit_validate(
        &self,
        _shard: &QueueKey,
        _refs: &[ClaimRef],
        _now: UtcTimestamp,
    ) -> EngineResult<()> {
        Err(EngineError::Unavailable)
    }

    /// Read the stored instance/state fence for `key` (`None`/`Ok(None)` == the unset value `0`). Used to
    /// validate a caller-supplied [`crate::InstanceFence`] before advancing it. Default: `Ok(None)`.
    fn instance_fence(&self, _shard: &QueueKey, _key: &[u8]) -> EngineResult<Option<u64>> {
        Ok(None)
    }

    /// Read an opaque non-work side record by key (recovery/audit read). Disjoint from work items, so it
    /// survives input finalization. Default: `Ok(None)`.
    fn side_record(&self, _shard: &QueueKey, _key: &[u8]) -> EngineResult<Option<Bytes>> {
        Ok(None)
    }

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

    fn range_scan(
        &self,
        _shard: &QueueKey,
        _request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        Err(EngineError::Unavailable)
    }

    fn grouped_aggregate(
        &self,
        _shard: &QueueKey,
        _request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        Err(EngineError::Unavailable)
    }

    fn declared_bucket_segment(
        &self,
        _shard: &QueueKey,
        _request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        Err(EngineError::Unavailable)
    }

    fn bounded_mutation(
        &mut self,
        _shard: &QueueKey,
        _request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationResponse> {
        Err(EngineError::Unavailable)
    }

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

    // -- recovery-on-open (ADR-012 P2) --------------------------------------

    /// The position this projection has ALREADY durably absorbed. The composition replays the durable log
    /// forward from here via [`LogStore::read_from`]. `None` (the default) is genesis — a fresh in-memory
    /// projection replays the whole log; a durable sqlite projection returns its persisted high-water so only
    /// the object-log tail beyond the snapshot is replayed (bead pqueue-8a76daad); a unified relational store
    /// has nothing to replay (its `apply` already wrote durably in the same transaction).
    fn recovery_high_water(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        Ok(None)
    }

    /// Cross-validate this projection's durably recorded object-log lineage against the log's actual
    /// `identity` (TD-004 "Async lineage validation") BEFORE the composition advertises this projection's
    /// high-water as a safe replay-skip point. Called once per durable shard during recovery-on-open, after
    /// the shard is hydrated but before any object-log tail replay. A hybrid projection whose recorded
    /// lineage does not descend from the log it is about to replay from — a recorded source epoch newer than
    /// the log currently records, or a logical high-water ahead of the log's committed head — MUST fail
    /// closed here (poison + `Storage` error) so the composition never serves from a projection image that
    /// diverges from the durable log. Default: `Ok` — a projection that records no lineage has nothing to
    /// validate.
    fn validate_recovery_lineage(&mut self, _identity: &LogLineageIdentity) -> EngineResult<()> {
        Ok(())
    }

    /// The projection's poison state for `shard` during recovery-on-open, or `None` when healthy. A
    /// projection that has latched poison — persistent checkpoint errors, corruption, or an unresolved
    /// replay-apply gap it cannot repair by waiting — MUST report it here so the composition stops serving
    /// (fail closed) instead of hydrating and advertising a divergent image (TD-004 §"Async apply debt,
    /// backpressure, and poison thresholds": poison). Default: `None` (the in-memory / relational
    /// projections carry no async-apply poison).
    fn recovery_poison(&self, _shard: &QueueKey) -> Option<String> {
        None
    }

    /// Whether the projection is under HARD async-apply backpressure for `shard` — its durable
    /// `sqlite_high_water` is lagging far enough that it MUST NOT be advertised as a safe replay-skip point
    /// (TD-004 "Recovery/high-water backpressure"). Unlike poison this is repairable by waiting, so the
    /// composition does not fail closed; it replays from an earlier authoritative source (genesis) rather
    /// than trusting the lagging high-water. Default: `false`.
    fn recovery_backpressured(&self, _shard: &QueueKey) -> bool {
        false
    }

    /// Enumerate the durable queue definitions this projection persists, for recovery-on-open. Default: empty
    /// (the in-memory projection persists nothing; the durable sqlite/relational projections override this).
    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(Vec::new())
    }

    /// Seed the composition's per-queue id-mint `counters` past every item id already materialized in the
    /// durable projection snapshot, so a push after a snapshot-tail reopen never re-mints an existing id.
    /// Default: no-op — the in-memory projection has no persisted snapshot, so its counters are restored by
    /// observing the ids in the replayed log instead.
    fn restore_counters(&self, _shard: &QueueKey, _counters: &QueueCounters) -> EngineResult<()> {
        Ok(())
    }
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
    /// Per-queue retained request-id cache for the vectorized claimed-work COMMIT path (epic
    /// pqueue-2201fd37) — the same `QueueIdempotencyCache` machinery as `idempotency`, but the cached outcome
    /// is the whole `Vec<EntryRecovery>` so a body+request_id replay returns the prior per-entry outcomes
    /// verbatim with NO double-write. Held under the same UoW lock so check + append + record stays atomic.
    commit_idempotency: HashMap<QueueKey, QueueIdempotencyCache<Vec<EntryRecovery>>>,
    cmd_seq: u64,
    /// Per-queue group-commit coordinators (ADR-012 P2). Empty + unused on the synchronous (atomic / OFF)
    /// path; populated only when the composition runs the ack-after-seal write path against a group-commit
    /// log. Protected by the SAME `Mutex<Inner>` as `log`/`projection` — no async lock.
    coords: HashMap<QueueKey, ShardCoord>,
}

/// Default recovery-window budget: the max durable-log tail (commands) a normal reopen replays beyond the
/// projection's recovery high-water before [`ComposedBackend::recover`] logs a recovery-window warning. The
/// durable projection advances its high-water inside the same transaction that applies each batch, so the
/// tail is normally a handful of commands; exceeding this suggests a projection that has fallen far behind
/// the log. (For a fresh in-memory projection the whole log is the "tail", so the budget is generous.)
pub const DEFAULT_RECOVERY_MAX_TAIL: u64 = 1_000_000;
const RECOVERY_READ_PAGE_LIMIT: usize = 8_192;

/// The one generic backend (ADR-012): `Backend = LogStore × ProjectionStore × ControlPlane`. Implements
/// every engine port by delegating to the three axes.
pub struct ComposedBackend<L, P, C> {
    inner: Mutex<Inner<L, P>>,
    control: C,
    /// Packed into every minted [`ItemId`] (ADR-009) so concurrent writers never collide. `0` default.
    node_id: u8,
    counters: QueueCounters,
    /// The durability class inherited from the log axis at assembly (TD-007 §2). Read once from
    /// `LogStore::durability_class` so the hot path never re-locks to decide whether an atomic-only port
    /// (upsert / update_fields / reschedule / commit_transition) is available.
    durability: DurabilityClass,
    /// Recovery-window budget (max tail commands) before [`Self::recover`] logs a recovery-window warning.
    recovery_max_tail: u64,
    /// Group-commit mode (ADR-012 P2), DEFAULT OFF. When `false` every write funnels through the synchronous
    /// `commit_locked` force-seal/append→apply path UNCHANGED. When `true` AND the log axis advertises
    /// `supports_group_commit()`, `push` co-buffers + acks-after-seal and read-modify-write ops force-seal the
    /// buffered batch before they select/apply (so they observe applied state under the one composed lock).
    group_commit: bool,
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ComposedBackend<L, P, C> {
    /// Assemble a backend from one of each axis.
    pub fn new(log: L, projection: P, control: C) -> Self {
        let durability = log.durability_class();
        Self {
            inner: Mutex::new(Inner {
                log,
                projection,
                idempotency: HashMap::new(),
                commit_idempotency: HashMap::new(),
                cmd_seq: 0,
                coords: HashMap::new(),
            }),
            control,
            node_id: 0,
            counters: QueueCounters::default(),
            durability,
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            group_commit: false,
        }
    }

    /// Override the recovery-window budget (max durable-log tail commands a reopen replays before a
    /// recovery-window warning is logged) — the composition-root form of `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS`.
    pub fn with_recovery_max_tail(mut self, max_tail: u64) -> Self {
        self.recovery_max_tail = max_tail;
        self
    }

    /// Enable the ack-after-seal group-commit write path (ADR-012 P2). DEFAULT OFF. Only takes effect for a
    /// log axis that advertises [`LogStore::supports_group_commit`]; on any other log the write path stays the
    /// synchronous `commit_locked` path regardless of this flag.
    pub fn with_group_commit(mut self, on: bool) -> Self {
        self.group_commit = on;
        self
    }

    /// Whether the composition runs the group-commit write path (the builder flag AND a group-commit-capable
    /// log). The server uses this to decide whether to spawn the externalized flush task.
    pub fn group_commit_enabled(&self) -> bool {
        self.group_commit
            && self
                .inner
                .lock()
                .expect("poisoned")
                .log
                .supports_group_commit()
    }

    /// The flush-task poll interval (ms): `gc_max_latency_ms()/4` (≥ 1), so a buffered-but-quiet segment
    /// seals within ~one latency window — the same cadence the monolith's `spawn_flusher` uses.
    pub fn group_commit_flush_interval_ms(&self) -> u64 {
        (self.inner.lock().expect("poisoned").log.gc_max_latency_ms() / 4).max(1)
    }

    /// Observability/test seam: run `f` against the log axis under the unit-of-work lock (e.g. to read the
    /// substrate's group-commit segment counters for the co-buffering proof).
    pub fn with_log<R>(&self, f: impl FnOnce(&L) -> R) -> R {
        f(&self.inner.lock().expect("poisoned").log)
    }

    /// Observability/test seam: run `f` against the projection axis under the unit-of-work lock.
    pub fn with_projection<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        f(&self.inner.lock().expect("poisoned").projection)
    }

    /// Read, emit, and durably advance the change-record tail cursor for one shard.
    pub fn emit_change_record_tail<S: crate::port::ChangeRecordSink>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<pqueue_core::OwnerId>,
    ) -> EngineResult<usize> {
        let (page, _cursor) = {
            let g = self.inner.lock().expect("composed backend poisoned");
            let cursor = g.log.emission_cursor(shard)?;
            let page = g.log.read_from(shard, cursor.clone(), limit)?;
            (page, cursor)
        };
        if page.entries.is_empty() {
            return Ok(0);
        }
        let mut records = Vec::new();
        for (position, env) in &page.entries {
            records.extend(command_envelope_change_records(
                shard,
                position,
                env,
                emitted_at,
                source_owner_id.clone(),
            ));
        }
        sink.emit(shard, &records)?;
        if let Some((position, _)) = page.entries.last() {
            let mut g = self.inner.lock().expect("composed backend poisoned");
            g.log.set_emission_cursor(shard, position.clone())?;
        }
        Ok(records.len())
    }

    // -- group-commit write-path helpers (ADR-012 P2) -----------------------

    /// Buffer `env` into the group-commit coordinator + substrate and return its [`SealSlot`] (the
    /// ack-after-seal handle). If a SIZE trigger fired during the enqueue the sealed batch is distributed
    /// immediately (applied + its waiters completed); otherwise the command stays buffered and is acked by a
    /// later size seal, the latency flusher, or a read-modify-write force-seal. Caller holds the `Inner` lock.
    fn gc_buffer(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        env: CommandEnvelope,
        expected_epoch: Option<u64>,
        now: UtcTimestamp,
    ) -> EngineResult<Arc<SealSlot>> {
        let slot = Arc::new(SealSlot::new());
        let now_ms = ts_to_ms(now);
        let resolved_epoch = match expected_epoch {
            Some(e) => e,
            None => inner.log.current_epoch(shard)?,
        };
        let enqueued = {
            let Inner { log, coords, .. } = &mut *inner;
            let coord = coords.entry(shard.clone()).or_default();
            if coord.pending.is_empty() {
                coord.seal_epoch = resolved_epoch;
            }
            coord.pending.push(env);
            coord.waiters.push(slot.clone());
            // Enqueue by reference (no per-command envelope clone on the hot path); the seal epoch is the
            // batch's, so co-buffered commands seal together under one epoch.
            log.gc_enqueue(
                shard,
                std::slice::from_ref(coord.pending.last().expect("just pushed")),
                coord.seal_epoch,
                now_ms,
            )
        };
        match enqueued {
            Ok(positions) if !positions.is_empty() => {
                // A size-triggered seal fired inside `gc_enqueue`; apply + complete the drained waiters.
                let _ = Self::gc_distribute(inner, shard, positions);
            }
            Ok(_) => {}
            // Fence/storage failure: the substrate discarded the buffer, so fail every registered waiter
            // (including this one) to keep `pending` consistent with the now-empty substrate buffer.
            Err(e) => Self::gc_fail_all(inner, shard, e),
        }
        Ok(slot)
    }

    /// Apply a freshly-sealed batch to the projection in ONE batch, advance the log high-water, then complete
    /// every waiter that contributed to it. `positions` pairs 1:1 with the front of `pending`/`waiters` (a
    /// seal drains the whole substrate buffer). Caller holds the `Inner` lock.
    fn gc_distribute(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        positions: Vec<CommandPosition>,
    ) -> EngineResult<()> {
        let Inner {
            log,
            projection,
            coords,
            ..
        } = inner;
        let Some(coord) = coords.get_mut(shard) else {
            return Ok(());
        };
        let n = positions
            .len()
            .min(coord.pending.len())
            .min(coord.waiters.len());
        let envelopes: Vec<CommandEnvelope> = coord.pending.drain(..n).collect();
        let waiters: Vec<Arc<SealSlot>> = coord.waiters.drain(..n).collect();
        let in_flight_claims: Vec<ItemId> = envelopes
            .iter()
            .filter_map(|env| match &env.command {
                QueueCommand::Claim(claim) => Some(claim.item_ids.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect();
        let result = (|| {
            let positions: Vec<CommandPosition> = positions.into_iter().take(n).collect();
            if let Some(last) = positions.last() {
                log.gc_advance_high_water(shard, last.clone())?;
            }
            projection.apply_live_owned(positions, envelopes)
        })();
        if let Some(coord) = coords.get_mut(shard) {
            for id in in_flight_claims {
                coord.in_flight_claims.remove(&id);
            }
            if coord.in_flight_claims.is_empty() {
                coord.in_flight_claim_tail = None;
            }
        }
        for w in waiters {
            w.complete(result.clone());
        }
        result
    }

    /// A seal failed (epoch fence / storage): the substrate discarded its buffer, so fail every registered
    /// waiter and clear `pending` to stay consistent with the now-empty substrate buffer.
    fn gc_fail_all(inner: &mut Inner<L, P>, shard: &QueueKey, err: EngineError) {
        if let Some(coord) = inner.coords.get_mut(shard) {
            coord.pending.clear();
            coord.in_flight_claims.clear();
            coord.in_flight_claim_tail = None;
            for w in coord.waiters.drain(..) {
                w.complete(Err(err.clone()));
            }
        }
    }

    /// Force-seal `shard`'s buffered batch (if any) and distribute it, so the projection reflects every prior
    /// co-buffered write BEFORE a read-modify-write op selects/validates against it. Caller holds the lock.
    fn gc_force_seal(inner: &mut Inner<L, P>, shard: &QueueKey, now_ms: i64) -> EngineResult<()> {
        let (seal_epoch, pending) = match inner.coords.get(shard) {
            Some(c) => (c.seal_epoch, !c.pending.is_empty()),
            None => (0, false),
        };
        if !pending {
            return Ok(());
        }
        match inner.log.gc_seal(shard, seal_epoch, now_ms) {
            Ok(positions) if !positions.is_empty() => Self::gc_distribute(inner, shard, positions),
            Ok(_) => Ok(()),
            Err(e) => {
                Self::gc_fail_all(inner, shard, e.clone());
                Err(e)
            }
        }
    }

    /// Synchronously commit a read-modify-write command batch on the group-commit log: buffer it, force-seal
    /// it (the buffer is empty — the caller force-sealed any prior batch first), advance the high-water, and
    /// apply it. The op observes its own write before returning (ack-after-seal, but synchronous because the
    /// caller already selected/validated under the lock). Caller holds the lock.
    fn gc_commit_sync_batch(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        envs: Vec<CommandEnvelope>,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        for env in &envs {
            validate_gate_command(false, &env.command)?;
            validate_request_replay_metadata(env)?;
        }
        if envs.is_empty() {
            return Ok(());
        }
        let now_ms = ts_to_ms(envs[0].created_at);
        let seal_epoch = match expected_epoch {
            Some(e) => e,
            None => inner.log.current_epoch(shard)?,
        };
        let mut positions = inner.log.gc_enqueue(shard, &envs, seal_epoch, now_ms)?;
        if positions.is_empty() {
            positions = inner.log.gc_seal(shard, seal_epoch, now_ms)?;
        }
        if let Some(last) = positions.last() {
            inner.log.gc_advance_high_water(shard, last.clone())?;
        }
        inner.projection.apply_live_owned(positions, envs)
    }

    fn gc_commit_sync(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        env: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        Self::gc_commit_sync_batch(inner, shard, vec![env], expected_epoch)
    }

    /// Whether the group-commit write path is active for this composition (the builder flag AND a capable
    /// log). Read on the hot path with the lock already held.
    fn gc_active(&self, inner: &Inner<L, P>) -> bool {
        self.group_commit && inner.log.supports_group_commit()
    }

    /// Seal every latency-due queue's buffered batch + distribute it (ADR-012 P2 externalized flusher). The
    /// runtime-bearing crate (`pqueue-server`, which has tokio) drives this on an interval at
    /// `group_commit_flush_interval_ms()`; the engine stays runtime-free. A no-op when group-commit is off.
    pub fn flush_tick(&self, now_ms: i64) -> EngineResult<()> {
        let mut g = self.inner.lock().expect("composed backend poisoned");
        if !self.gc_active(&g) {
            return Ok(());
        }
        let shards: Vec<(QueueKey, u64)> = g
            .coords
            .iter()
            .filter(|(_, c)| !c.pending.is_empty())
            .map(|(k, c)| (k.clone(), c.seal_epoch))
            .collect();
        for (shard, seal_epoch) in shards {
            match g.log.gc_flush_due(&shard, seal_epoch, now_ms) {
                Ok(positions) if !positions.is_empty() => {
                    Self::gc_distribute(&mut g, &shard, positions)?;
                }
                Ok(_) => {}
                Err(e) => Self::gc_fail_all(&mut g, &shard, e),
            }
        }
        Ok(())
    }

    /// Drain deferred projection work, if the projection supports it. This is separate from `flush_tick` so
    /// latency-sensitive manifest sealing does not wait on a durable projection checkpoint.
    pub fn flush_deferred_projection(&self) -> EngineResult<()> {
        self.inner
            .lock()
            .expect("composed backend poisoned")
            .projection
            .flush_deferred()
    }

    /// Best-effort deferred projection drain for background flusher tasks.
    ///
    /// Unlike [`Self::flush_deferred_projection`], this never waits for the composed backend mutex. If a
    /// push/claim/finalize is active, the background checkpoint simply skips this tick and tries again on the
    /// next cadence. Explicit catch-up/recovery tests can still call the blocking method above.
    pub fn try_flush_deferred_projection(&self) -> EngineResult<bool> {
        match self.inner.try_lock() {
            Ok(mut g) => {
                g.projection.flush_deferred()?;
                Ok(true)
            }
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(EngineError::Storage("composed backend poisoned".into()))
            }
        }
    }

    /// Recovery-on-open (ADR-012 P2): rebuild the in-memory derived state from the durable substrates so a
    /// reopened durable composition recovers identically to its monolith — WITHOUT a re-`create_queue`. For
    /// every durable queue (enumerated from the projection's then the log's durable catalog) this:
    ///
    /// 1. repopulates the in-process control plane + ensures the log/projection shards exist (the durable
    ///    epoch/fence in the log is preserved, never reset);
    /// 2. seeds the id-mint counters from the durable projection snapshot ([`ProjectionStore::restore_counters`]);
    /// 3. replays the durable log forward from the projection's [`ProjectionStore::recovery_high_water`]
    ///    (genesis for a fresh in-memory projection; the snapshot tail for a durable sqlite projection; nothing
    ///    for a unified relational store), applying each batch through [`ProjectionStore::apply`] and observing
    ///    the minted ids + the command sequence so post-reopen mints never collide.
    ///
    /// A fresh (`:memory:` / never-written) composition has empty durable catalogs, so this is a cheap no-op.
    /// Durable constructors call this; the in-process memory composition does not need it.
    pub fn recover(self) -> EngineResult<Self> {
        self.run_recovery()?;
        Ok(self)
    }

    fn run_recovery(&self) -> EngineResult<()> {
        // 1. Gather the durable definitions, projection catalog first then log catalog, deduped by key.
        let definitions: Vec<QueueDefinition> = {
            let g = self.inner.lock().expect("composed backend poisoned");
            let mut seen: std::collections::HashSet<QueueKey> = std::collections::HashSet::new();
            let mut defs = Vec::new();
            for def in g
                .projection
                .recover_definitions()?
                .into_iter()
                .chain(g.log.recover_definitions()?)
            {
                let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
                if seen.insert(key) {
                    defs.push(def);
                }
            }
            defs
        };
        if definitions.is_empty() {
            return Ok(());
        }

        let mut max_cmd_seq: Option<u64> = None;
        for def in definitions {
            let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
            // Repopulate the in-process control plane (idempotent for a compatible re-create).
            self.control.create_queue(def.clone())?;
            let mut g = self.inner.lock().expect("composed backend poisoned");
            let Inner {
                log,
                projection,
                idempotency,
                ..
            } = &mut *g;
            log.ensure_shard(&key)?;
            projection.ensure_shard(&def)?;
            // Seed counters from the durable projection snapshot (no-op for the in-memory projection).
            projection.restore_counters(&key, &self.counters)?;
            // Cross-validate the (now hydrated) durable projection's recorded object-log lineage against the
            // log's actual identity (TD-004 async lineage validation), BEFORE trusting its high-water as a
            // replay-skip point. A hybrid projection whose recorded lineage does not descend from this log
            // fails closed here (the in-memory / relational projections record no lineage → no-op default).
            let identity = LogLineageIdentity {
                shard: key.clone(),
                current_epoch: log.current_epoch(&key)?,
                high_water: log.high_water(&key)?,
            };
            projection.validate_recovery_lineage(&identity)?;
            // Fold async-apply poison / hard-backpressure health into the replay-start decision (TD-004
            // §backpressure/poison): a poisoned projection fails closed here (unresolved replay poison must
            // stop serving; high-water must not advance past poison), and a hard-backpressured one replays
            // from genesis rather than trusting its lagging high-water as a safe skip point.
            let recovery_poison = projection.recovery_poison(&key);
            let hard_backpressure = projection.recovery_backpressured(&key);
            if self.durability == DurabilityClass::EventualApply {
                Self::rebuild_push_idempotency_from_log(
                    log,
                    idempotency,
                    &key,
                    def.request_id_retention_ms,
                )?;
            }
            // Replay the durable log tail from the projection's recovery high-water (genesis when `None`),
            // after the poison/backpressure gate above resolves whether that high-water is trustworthy.
            let recorded_high_water = projection.recovery_high_water(&key)?;
            let mut from = match resolve_recovery_start(
                recovery_poison.as_deref(),
                hard_backpressure,
                recorded_high_water,
            )? {
                RecoveryStart::FromHighWater(pos) => pos,
                RecoveryStart::FromGenesis => None,
            };
            let mut tail: u64 = 0;
            loop {
                let page = log.read_from(&key, from.clone(), RECOVERY_READ_PAGE_LIMIT)?;
                if !page.entries.is_empty() {
                    let entries = page.entries;
                    let mut positions = Vec::with_capacity(entries.len());
                    let mut envelopes = Vec::with_capacity(entries.len());
                    for (pos, env) in entries {
                        positions.push(pos);
                        envelopes.push(env);
                    }
                    for env in &envelopes {
                        for id in &env.item_ids {
                            self.counters.observe(&key, *id);
                        }
                        // The composition mints `cmp-{node}-{n}` command ids; resume past the highest replayed
                        // sequence so a post-reopen append never re-mints an existing command id.
                        if let Some(n) = env
                            .command_id
                            .0
                            .rsplit('-')
                            .next()
                            .and_then(|s| s.parse::<u64>().ok())
                        {
                            max_cmd_seq = Some(max_cmd_seq.map_or(n, |m| m.max(n)));
                        }
                    }
                    tail += positions.len() as u64;
                    projection.apply_recovery(&positions, &envelopes)?;
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
            if tail > self.recovery_max_tail {
                eprintln!(
                    "[recovery] composed backend tail for {}:{} replayed {tail} commands beyond the \
                     projection high-water (budget {}); the projection may have fallen behind the log",
                    key.tenant_id.as_str(),
                    key.queue_id.as_str(),
                    self.recovery_max_tail,
                );
            }
        }
        if let Some(m) = max_cmd_seq {
            let mut g = self.inner.lock().expect("composed backend poisoned");
            g.cmd_seq = g.cmd_seq.max(m + 1);
        }
        Ok(())
    }

    fn rebuild_push_idempotency_from_log(
        log: &L,
        idempotency: &mut HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
        shard: &QueueKey,
        retention_ms: u64,
    ) -> EngineResult<()> {
        let mut from = None;
        loop {
            let page = log.read_from(shard, from.clone(), RECOVERY_READ_PAGE_LIMIT)?;
            for (_, env) in &page.entries {
                let Some(request_id) = &env.request_id else {
                    continue;
                };
                let QueueCommand::Push(push) = &env.command else {
                    continue;
                };
                let fingerprint = env
                    .request_fingerprint
                    .map(BodyHash)
                    .unwrap_or(push_item_body_hash(&push.items)?);
                let expires_at = request_expires_at(env.created_at, retention_ms);
                idempotency.entry(shard.clone()).or_default().record(
                    request_id.clone(),
                    fingerprint,
                    match &env.request_outcome {
                        Some(RequestOutcome::Push { item_ids }) => item_ids.clone(),
                        None => env.item_ids.clone(),
                    },
                    expires_at,
                );
            }
            match page.next {
                Some(next) => from = Some(next),
                None => break,
            }
        }
        Ok(())
    }

    /// Whether the composition offers the atomic append+apply boundary the atomic-only ports require
    /// (upsert / update_fields / reschedule / commit_transition). An eventual-apply log refuses them.
    fn is_atomic(&self) -> bool {
        self.durability == DurabilityClass::Atomic
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
            request_fingerprint: None,
            request_outcome: None,
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
        Self::commit_locked_batch(inner, shard, vec![env], expected_epoch)
    }

    fn commit_locked_batch(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        envs: Vec<CommandEnvelope>,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        for env in &envs {
            validate_gate_command(false, &env.command)?;
        }
        if envs.is_empty() {
            return Ok(());
        }
        let epoch = inner.log.current_epoch(shard)?;
        // ADR-009 / TD-003: an owner that supplies its cached acquire-time epoch (`Some`) is fenced here if
        // superseded; `None` is the degenerate sole-owner path (stamp current, never fence).
        if expected_epoch.is_some_and(|e| e != epoch) {
            return Err(EngineError::EpochFenced);
        }
        let positions = inner.log.append(shard, &envs, epoch)?;
        inner.projection.apply_live(&positions, &envs)
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
    #[derive(serde::Serialize)]
    struct CanonicalPushSpec<'a> {
        client_item_key: Option<&'a ClientItemKey>,
        priority: &'a Option<PriorityValue>,
        not_before: &'a Option<UtcTimestamp>,
        group_key: &'a Option<GroupKey>,
        payload: &'a Option<Bytes>,
        fields: &'a BTreeMap<String, Bytes>,
        metadata: &'a Metadata,
        cohort_size: &'a Option<u64>,
        gate_keys: &'a Vec<String>,
        entity: &'a Option<serde_json::Value>,
    }
    let canonical: Vec<_> = items
        .iter()
        .map(|item| CanonicalPushSpec {
            client_item_key: item.client_item_key.as_ref(),
            priority: &item.priority,
            not_before: &item.not_before,
            group_key: &item.group_key,
            payload: &item.payload,
            fields: &item.fields,
            metadata: &item.metadata,
            cohort_size: &item.cohort_size,
            gate_keys: &item.gate_keys,
            entity: &item.entity,
        })
        .collect();
    push_body_hash_canonical(&canonical)
}

/// The recovery twin of [`push_body_hash`]. Committed log entries contain [`PushItem`]s, not the caller's
/// original [`PushSpec`]s: assigned `item_id` and `max_attempts` are excluded, and a defaulted
/// `client_item_key == item_id` is normalized back to `None` so same-body retries with omitted keys replay
/// after restart.
fn push_item_body_hash(items: &[PushItem]) -> EngineResult<BodyHash> {
    #[derive(serde::Serialize)]
    struct CanonicalPushItem<'a> {
        client_item_key: Option<&'a ClientItemKey>,
        priority: &'a Option<PriorityValue>,
        not_before: &'a Option<UtcTimestamp>,
        group_key: &'a Option<GroupKey>,
        payload: &'a Option<Bytes>,
        fields: &'a BTreeMap<String, Bytes>,
        metadata: &'a Metadata,
        cohort_size: &'a Option<u64>,
        gate_keys: &'a Vec<String>,
        entity: &'a Option<serde_json::Value>,
    }
    let canonical: Vec<_> = items
        .iter()
        .map(|item| CanonicalPushItem {
            client_item_key: (item.client_item_key.as_str() != item.item_id.to_string())
                .then_some(&item.client_item_key),
            priority: &item.priority,
            not_before: &item.not_before,
            group_key: &item.group_key,
            payload: &item.payload,
            fields: &item.fields,
            metadata: &item.metadata,
            cohort_size: &item.cohort_size,
            gate_keys: &item.gate_keys,
            entity: &item.entity_document,
        })
        .collect();
    push_body_hash_canonical(&canonical)
}

fn push_body_hash_canonical<T: serde::Serialize>(items: &[T]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

/// Stable body fingerprint for the vectorized commit path: a non-cryptographic hash over the serialized
/// commit entries (the request_id is the cache KEY, not part of the body). A different body under the same
/// request id is a `RequestIdConflict`; an equal body replays the prior per-entry outcomes.
fn commit_body_hash(entries: &[crate::port::CommitTransitionEntry]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

/// Project the retained per-entry recovery records into the public per-entry outcomes (the commit return /
/// replay value). The recovery record is the superset (it ALSO carries the consumed input id, instance
/// fence, and side-record keys for `explain_commit`).
fn outcomes_from_recovery(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
    recovery
        .iter()
        .map(|r| match &r.status {
            CommitEntryStatus::Committed => CommitEntryOutcome::Committed {
                lifecycle_item_ids: r.lifecycle_item_ids.clone(),
            },
            CommitEntryStatus::Rejected(e) => CommitEntryOutcome::Rejected(e.clone()),
        })
        .collect()
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
        self.projection.apply_live(positions, commands)
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> Backend for ComposedBackend<L, P, C> {
    fn durability_class(&self) -> DurabilityClass {
        self.durability
    }

    /// The authoritative-commit capabilities (Snorri StateStore boundary, epic pqueue-2201fd37). The
    /// composition advertises the FULL vectorized-commit guarantees iff BOTH axes support it: the projection
    /// materializes the commit-class read model (`supports_commit_transition`) AND the log gives an atomic
    /// append+apply boundary. Otherwise it advertises the all-false default so a consumer (Snorri) rejects it
    /// before activation. This reaches parity with the monolithic `MemoryBackend` for the composed memory
    /// backend (`MemoryLog × InMemoryProjection`).
    fn commit_capabilities(&self) -> CommitCapabilities {
        let supports = {
            let g = self.inner.lock().expect("composed backend poisoned");
            g.projection.supports_commit_transition()
        };
        if supports && self.is_atomic() {
            CommitCapabilities {
                atomic_transition_commit: true,
                vectorized_commit: true,
                lease_validation: true,
                retained_commit_idempotency: true,
                non_work_side_records: true,
                authoritative_recovery_reads: true,
                delayed_awaits_timers: true,
                durability_class: self.durability,
                consistency: "atomic append+apply under one composed unit-of-work lock",
            }
        } else {
            CommitCapabilities::default()
        }
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
                // Record the definition in the log's durable catalog so a reopened composition can recover
                // this queue without a re-`create_queue` (no-op for in-process / unified-relational logs).
                log.persist_definition(&outcome.definition)?;
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
        // Prologue (shared with the OFF path): build the push items + envelope. A pre-commit failure resolves
        // immediately. The ON path then co-buffers the envelope and returns an ack-after-seal `SealFuture`.
        let prepared = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let def = self.control.queue_definition(shard)?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            for item in &items {
                validate_entity(schema.as_ref(), item.entity.as_ref())?;
            }
            let max_attempts = def.retry_policy.max_attempts;
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
            Ok::<_, EngineError>((g, env, ids))
        })();
        let (mut g, env, ids) = match prepared {
            Ok(v) => v,
            Err(e) => return AckFuture::ready(Err(e)),
        };
        if self.gc_active(&g) {
            // Group-commit: co-buffer + register a SealSlot, drop the guard, return an ack-after-seal future.
            let slot = match Self::gc_buffer(&mut g, shard, env, expected_epoch, now) {
                Ok(slot) => slot,
                Err(e) => return AckFuture::ready(Err(e)),
            };
            drop(g);
            AckFuture::seal(slot, ids)
        } else {
            let result = Self::commit_locked(&mut g, shard, env, expected_epoch).map(|()| ids);
            drop(g);
            AckFuture::ready(result)
        }
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        // The request-id'd push is NOT the co-buffering hot path: in group-commit mode it force-seals the
        // prior buffer + commits synchronously (`gc_commit_sync`) so the retained idempotency record is only
        // written AFTER a successful durable commit (a deferred-seal failure must leave NO replay entry).
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            let def = self.control.queue_definition(shard)?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            for item in &items {
                validate_entity(schema.as_ref(), item.entity.as_ref())?;
            }
            let max_attempts = def.retry_policy.max_attempts;
            let expires_at = request_expires_at(now, def.request_id_retention_ms);
            let mut g = self.inner.lock().expect("poisoned");
            let gc = self.gc_active(&g);
            if gc {
                Self::gc_force_seal(&mut g, shard, ts_to_ms(now))?;
            }
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
                request_fingerprint: Some(fingerprint.0),
                request_outcome: Some(RequestOutcome::Push {
                    item_ids: ids.clone(),
                }),
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            if gc {
                Self::gc_commit_sync(&mut g, shard, env, expected_epoch)?;
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            }
            g.idempotency.entry(shard.clone()).or_default().record(
                request_id,
                fingerprint,
                ids.clone(),
                expires_at,
            );
            Ok(ids)
        })();
        AckFuture::ready(result)
    }
}

// ---------------------------------------------------------------------------
// ClaimPort
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ClaimPort for ComposedBackend<L, P, C> {
    #[allow(refining_impl_trait)]
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = EngineResult<Claimed>> + Send + '_>> {
        enum ClaimStart {
            Ready(Claimed),
            Wait {
                slot: Arc<SealSlot>,
                shard: QueueKey,
                candidates: Vec<ItemId>,
            },
        }

        let result = (|| {
            // Resolve the claim unit from the compatibility options. Item-level (the default) is unchanged;
            // this log-replay composition refuses richer claim units with `Unavailable` rather than
            // silently downgrading them (BQ-14a).
            let def = self.control.queue_definition(&req.shard)?;
            if req.compatibility != ClaimCompatibility::default() {
                require_item_level_claim(&req.compatibility, req.max_items as u64, &def)?;
            }
            let strict_candidate_cursor =
                def.ordering_mode == OrderingMode::Strict || def.max_rank_error == 0;
            let mut g = self.inner.lock().expect("poisoned");
            let gc = self.gc_active(&g);
            if gc {
                // Claims must observe earlier buffered data-plane writes that can change eligibility (notably
                // pushes), but pending claims are already represented by `in_flight_claims` below and should
                // remain batchable instead of being force-sealed one command at a time.
                let must_observe_prior_writes = g.coords.get(&req.shard).is_some_and(|coord| {
                    coord
                        .pending
                        .iter()
                        .any(|env| !matches!(env.command, QueueCommand::Claim(_)))
                });
                if must_observe_prior_writes {
                    Self::gc_force_seal(&mut g, &req.shard, ts_to_ms(req.now))?;
                }
            }
            let candidates: Vec<ItemId> = if gc && strict_candidate_cursor {
                let after = g
                    .coords
                    .get(&req.shard)
                    .and_then(|coord| coord.in_flight_claim_tail);
                g.projection
                    .eligible_candidates_after(&req.shard, req.now, after, req.max_items)?
            } else {
                let in_flight_claims = g
                    .coords
                    .get(&req.shard)
                    .map(|coord| coord.in_flight_claims.clone())
                    .unwrap_or_default();
                let candidate_limit = req.max_items.saturating_add(in_flight_claims.len()).max(1);
                g.projection
                    .eligible_candidates(&req.shard, req.now, candidate_limit)?
                    .into_iter()
                    .filter(|id| !in_flight_claims.contains(id))
                    .take(req.max_items)
                    .collect()
            };
            if candidates.is_empty() {
                return Ok(ClaimStart::Ready(Claimed::default()));
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
            if gc {
                let coord = g.coords.entry(req.shard.clone()).or_default();
                coord.in_flight_claims.extend(candidates.iter().copied());
                coord.in_flight_claim_tail = candidates.last().copied();
                let slot = Self::gc_buffer(&mut g, &req.shard, env, req.expected_epoch, req.now)?;
                return Ok(ClaimStart::Wait {
                    slot,
                    shard: req.shard.clone(),
                    candidates,
                });
            } else {
                Self::commit_locked(&mut g, &req.shard, env, req.expected_epoch)?;
            }
            let items = g.projection.render_claimed(&req.shard, &candidates)?;
            debug_assert_eq!(
                items.len(),
                candidates.len(),
                "leased candidate failed to render"
            );
            Ok(ClaimStart::Ready(Claimed {
                items,
                ..Default::default()
            }))
        })();
        match result {
            Ok(ClaimStart::Wait {
                slot,
                shard,
                candidates,
            }) => Box::pin(async move {
                SealWait::new(slot).await?;
                let g = self.inner.lock().expect("poisoned");
                let items = g.projection.render_claimed(&shard, &candidates)?;
                debug_assert_eq!(
                    items.len(),
                    candidates.len(),
                    "sealed claim failed to render"
                );
                Ok(Claimed {
                    items,
                    ..Default::default()
                })
            }),
            Ok(ClaimStart::Ready(claimed)) => Box::pin(std::future::ready(Ok(claimed))),
            Err(e) => Box::pin(std::future::ready(Err(e))),
        }
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
            // Upsert (`ReplacePending`) needs the atomic look-then-replace boundary; an eventual-apply log
            // refuses it (parity with the monolith's `upsert_is_unavailable`), rather than splitting it.
            if !self.is_atomic() {
                return Err(EngineError::Unavailable);
            }
            let def = self.control.queue_definition(shard)?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            let max_attempts = def.retry_policy.max_attempts;
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
                    g.projection.index_validate(
                        shard,
                        &item.item_id,
                        &item.fields,
                        item.entity_document.as_ref(),
                        None,
                    )?;
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
            let gc = self.gc_active(&g);
            g.projection.finalize_validate(shard, &outcomes)?;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            if gc {
                let slot = Self::gc_buffer(&mut g, shard, env, expected_epoch, now)?;
                Ok(Some(slot))
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                Ok(None)
            }
        })();
        match result {
            Ok(Some(slot)) => AckFuture::seal(slot, ()),
            Ok(None) => AckFuture::ready(Ok(())),
            Err(e) => AckFuture::ready(Err(e)),
        }
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
            let gc = self.gc_active(&g);
            if gc {
                Self::gc_force_seal(&mut g, shard, ts_to_ms(now))?;
            }
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
            if gc {
                Self::gc_commit_sync(&mut g, shard, env, expected_epoch)?;
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            }
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
            let gc = self.gc_active(&g);
            if gc {
                Self::gc_force_seal(&mut g, shard, ts_to_ms(now))?;
            }
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
            if gc {
                Self::gc_commit_sync(&mut g, shard, env, expected_epoch)?;
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            }
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
            let gc = self.gc_active(&g);
            if gc {
                Self::gc_force_seal(&mut g, shard, ts_to_ms(now))?;
            }
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
            if gc {
                Self::gc_commit_sync(&mut g, shard, env, expected_epoch)?;
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            }
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
            // In-place field/payload merge is an atomic-class feature; an eventual-apply log refuses it.
            if !self.is_atomic() {
                return Err(EngineError::Unavailable);
            }
            let def = self.control.queue_definition(shard)?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            let mut g = self.inner.lock().expect("poisoned");
            g.projection
                .update_fields_validate(shard, &item_id, expected_item_version)?;
            g.projection
                .index_validate_update(shard, &item_id, &field_ops, entity.as_ref())?;
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
            let gc = self.gc_active(&g);
            if gc {
                Self::gc_force_seal(&mut g, shard, ts_to_ms(now))?;
            }
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
            if gc {
                Self::gc_commit_sync(&mut g, shard, env, expected_epoch)?;
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            }
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
            let gc = self.gc_active(&g);
            if gc {
                // Force-seal every queue's buffered batch so the lease-expiry sweep observes applied state.
                let shards: Vec<QueueKey> = g
                    .coords
                    .iter()
                    .filter(|(_, c)| !c.pending.is_empty())
                    .map(|(k, _)| k.clone())
                    .collect();
                for shard in shards {
                    Self::gc_force_seal(&mut g, &shard, ts_to_ms(now))?;
                }
            }
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
                if gc {
                    Self::gc_commit_sync(&mut g, &shard, env, None)?;
                } else {
                    Self::commit_locked(&mut g, &shard, env, None)?;
                }
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

impl<L, P, C> HistoricalProjectionRead for ComposedBackend<L, P, C>
where
    L: LogStore,
    P: AsOfProjectionStore,
    C: ControlPlane,
{
    type AsOfProjection = P::AsOfProjection;

    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<CommandPosition>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .log
            .current_position(shard);
        std::future::ready(result)
    }

    fn read_as_of<T, F>(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send,
    {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let definition = self.control.queue_definition(shard)?;
            let snapshot_ref = g.log.snapshot_at_or_before(shard, &position)?;
            let snapshot = match snapshot_ref.as_ref() {
                Some(snapshot_ref) => Some(g.log.read_snapshot(snapshot_ref)?),
                None => None,
            };
            let mut as_of = g.projection.reconstruct_as_of(&definition, snapshot)?;
            let mut from = snapshot_ref.map(|s| s.position);
            loop {
                let page = g.log.read_from(shard, from.clone(), RECOVERY_READ_PAGE_LIMIT)?;
                if page.entries.is_empty() {
                    break;
                }
                let mut positions = Vec::new();
                let mut envelopes = Vec::new();
                let mut reached_target = false;
                for (entry_position, env) in page.entries {
                    if entry_position == position || entry_position.precedes(&position) {
                        positions.push(entry_position.clone());
                        envelopes.push(env);
                    } else {
                        reached_target = true;
                        break;
                    }
                }
                if !positions.is_empty() {
                    as_of.apply_recovery(&positions, &envelopes)?;
                }
                if reached_target || page.next.is_none() {
                    break;
                }
                from = page.next;
            }
            query(&as_of)
        })();
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
// ReschedulePort — atomic in-place priority/not_before change (rides the UpdateFields command)
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ReschedulePort for ComposedBackend<L, P, C> {
    fn reschedule(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        set_priority: ScheduleUpdate<PriorityValue>,
        set_not_before: ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            // Reschedule is an atomic-class feature; an eventual-apply log refuses it (no eligibility re-key).
            if !self.is_atomic() {
                return Err(EngineError::Unavailable);
            }
            let mut g = self.inner.lock().expect("poisoned");
            // Same pre-commit gate as a field update: an absent / terminal / superseded / fenced id or a
            // version mismatch rejects and nothing is appended.
            g.projection
                .update_fields_validate(shard, &item_id, expected_item_version)?;
            // Reschedule rides the UpdateFields command with an empty field/payload delta — only the
            // priority/not_before reschedule is carried. The projection re-keys eligibility on a reprice.
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops: BTreeMap::new(),
                    payload: PayloadUpdate::Keep,
                    set_priority,
                    set_not_before,
                    set_entity_document: None,
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
// HotProjectionQueryPort (API-004) — no backend in epic pqueue-45e13e4d implements the hot projection
// query substrate yet; the composed family takes the all-default (all-`Unavailable`) implementation
// until a follow-on bead wires real range-scan/aggregate/mutation execution.
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::HotProjectionQueryPort
    for ComposedBackend<L, P, C>
{
    fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
        QueryCapabilityFlags {
            range_scan: true,
            grouped_aggregate: true,
            declared_bucket_segment: true,
            bounded_mutation: true,
            claim_by_query: true,
            side_record_query: false,
        }
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<RangeScanResponse>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .range_scan(shard, request);
        std::future::ready(result)
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .grouped_aggregate(shard, request);
        std::future::ready(result)
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .declared_bucket_segment(shard, request);
        std::future::ready(result)
    }

    fn bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
    ) -> impl std::future::Future<Output = EngineResult<BoundedMutationResponse>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            g.projection.bounded_mutation(shard, request)
        };
        std::future::ready(result)
    }

    fn claim_by_query(
        &self,
        shard: &QueueKey,
        request: ClaimByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let page_size = request.max_items.clamp(1, 1_000);
            let mut cursor = None;
            let mut item_ids = Vec::new();
            while item_ids.len() < request.max_items as usize {
                let page = g.projection.range_scan(
                    shard,
                    RangeScanRequest {
                        index: request.index.clone(),
                        filters: request.filters.clone(),
                        order_by: vec![request.order_by.clone()],
                        page_size,
                        cursor,
                    },
                )?;
                item_ids.extend(page.rows.into_iter().map(|row| row.item_id));
                item_ids.truncate(request.max_items as usize);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }

            if item_ids.is_empty() {
                return Ok(Claimed::default());
            }

            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let lease_token =
                LeaseToken::new(format!("cbq-{nanos}")).expect("generated lease token is valid");
            let created_at = UtcTimestamp::new(
                (nanos / 1_000_000_000) as i64,
                (nanos % 1_000_000_000) as u32,
            )
            .expect("valid timestamp");
            let lease_nanos = nanos + u128::from(request.lease_duration_ms) * 1_000_000;
            let lease_expires_at = UtcTimestamp::new(
                (lease_nanos / 1_000_000_000) as i64,
                (lease_nanos % 1_000_000_000) as u32,
            )
            .expect("valid lease timestamp");
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::Claim(ClaimCommand {
                    item_ids: item_ids.clone(),
                    lease_token: lease_token.clone(),
                    lease_expires_at,
                }),
                item_ids.clone(),
                created_at,
            );
            Self::commit_locked(&mut g, shard, env, None)?;
            let items = g.projection.render_claimed(shard, &item_ids)?;
            debug_assert_eq!(
                items.len(),
                item_ids.len(),
                "every queried claim candidate must render"
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
// CommitTransitionPort — the authoritative vectorized claimed-work commit (Snorri StateStore boundary,
// ADR-009 / epic pqueue-2201fd37), ported generically onto the composition via `commit_locked`.
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> CommitTransitionPort
    for ComposedBackend<L, P, C>
{
    /// The whole operation runs under ONE unit-of-work lock so request-id check + per-entry validate +
    /// append + apply + record is a single atomic unit. Behaviorally identical to the monolithic
    /// `MemoryBackend::commit_transition` (proven by the parity tests against `composed_memory_backend`).
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        let result = (|| {
            let CommitTransition {
                request_id,
                entries,
            } = transition;
            let fingerprint = commit_body_hash(&entries)?;
            // The commit boundary requires BOTH the atomic append+apply log AND a commit-class projection;
            // otherwise refuse the whole operation rather than splitting/faking it (Snorri rejects the
            // backend before activation via `commit_capabilities`).
            let (max_attempts, retention, schema) = {
                let def = self.control.queue_definition(shard)?;
                let schema = def
                    .entity_schema
                    .as_ref()
                    .and_then(|esd| esd.entity_schema.as_ref())
                    .map(compile_entity_schema)
                    .transpose()?;
                (
                    def.retry_policy.max_attempts,
                    def.request_id_retention_ms,
                    schema,
                )
            };
            let mut g = self.inner.lock().expect("poisoned");
            if !self.is_atomic() || !g.projection.supports_commit_transition() {
                return Err(EngineError::Unavailable);
            }

            // (1) Request-id idempotency over the WHOLE commit body. A retained body+id REPLAYS the prior
            //     per-entry outcomes (no re-write); a different body under that id is `RequestIdConflict`.
            if let Some(rid) = &request_id {
                match g
                    .commit_idempotency
                    .entry(shard.clone())
                    .or_default()
                    .check(rid, fingerprint, now)
                {
                    IdempotencyDecision::Replay(recovery) => {
                        return Ok(outcomes_from_recovery(&recovery));
                    }
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }

            // (2) Per entry: validate the lease-token + version-fenced claim_ref AND the optional instance
            //     fence, then commit the entry's side-records + fence advance + lifecycle push + input
            //     finalize atomically. A rejected entry mutates nothing.
            let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
            for entry in entries {
                let claim_ref = entry.claim_ref;
                let consumed_input_id = claim_ref.item_id;
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };

                if let Err(e) =
                    g.projection
                        .commit_validate(shard, std::slice::from_ref(&claim_ref), now)
                {
                    recovery.push(reject(e));
                    continue;
                }

                // C6: validate the caller-supplied instance fence against the stored fence (absent == 0).
                if let Some(fence) = &entry.instance_fence {
                    let stored = g
                        .projection
                        .instance_fence(shard, &fence.instance_key)?
                        .unwrap_or(0);
                    if let Err(e) = validate_instance_fence(stored, fence) {
                        recovery.push(reject(e));
                        continue;
                    }
                }

                // Capture the recovery facts BEFORE moving the entry's records into commands.
                let side_record_keys: Vec<Vec<u8>> =
                    entry.side_records.iter().map(|r| r.key.clone()).collect();
                let instance = entry
                    .instance_fence
                    .as_ref()
                    .map(|f| (f.instance_key.clone(), f.next));

                // Build the entry's envelopes WITHOUT committing yet, so a build-time rejection (e.g. a
                // unique-index conflict on a lifecycle item) leaves nothing mutated. The caller's request_id
                // propagates into every envelope.
                let mut envelopes: Vec<CommandEnvelope> = Vec::new();
                let mk_env = |g: &mut Inner<L, P>, command: QueueCommand, item_ids: Vec<ItemId>| {
                    let command_id = Self::next_command_id(g, self.node_id);
                    CommandEnvelope {
                        command_id,
                        request_id: request_id.clone(),
                        request_fingerprint: None,
                        request_outcome: None,
                        item_ids,
                        command,
                        checksum: CommandChecksum(0),
                        created_at: now,
                    }
                };
                if !entry.side_records.is_empty() {
                    let e = mk_env(
                        &mut g,
                        QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: entry.side_records,
                        }),
                        Vec::new(),
                    );
                    envelopes.push(e);
                }
                if let Some(fence) = entry.instance_fence {
                    let e = mk_env(
                        &mut g,
                        QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                            instance_key: fence.instance_key,
                            expected: fence.expected,
                            next: fence.next,
                        }),
                        Vec::new(),
                    );
                    envelopes.push(e);
                }
                let mut lifecycle_item_ids = Vec::new();
                if !entry.lifecycle_items.is_empty() {
                    if let Some(e) = entry.lifecycle_items.iter().find_map(|item| {
                        validate_entity(schema.as_ref(), item.entity.as_ref()).err()
                    }) {
                        recovery.push(reject(e));
                        continue;
                    }
                    let epoch = expected_epoch.unwrap_or(0);
                    let counter_base =
                        self.counters
                            .reserve(shard, epoch, entry.lifecycle_items.len() as u32);
                    let (push_items, ids) = build_push_items(
                        entry.lifecycle_items,
                        epoch,
                        self.node_id,
                        counter_base,
                        max_attempts,
                    );
                    if let Err(e) = g.projection.index_validate_push(shard, &push_items) {
                        recovery.push(reject(e));
                        continue;
                    }
                    lifecycle_item_ids = ids.clone();
                    let e = mk_env(
                        &mut g,
                        QueueCommand::Push(PushCommand { items: push_items }),
                        ids,
                    );
                    envelopes.push(e);
                }
                let e = mk_env(
                    &mut g,
                    QueueCommand::Finalize(FinalizeCommand {
                        outcomes: vec![FinalizeOutcome::new(claim_ref.item_id, entry.finalize)],
                    }),
                    vec![claim_ref.item_id],
                );
                envelopes.push(e);

                // Commit the entry's envelopes under the held lock as one append batch. The epoch cannot
                // change while we hold the lock, so either the append fences (EpochFenced, before any
                // mutation) or all of the entry's writes commit and apply together.
                Self::commit_locked_batch(&mut g, shard, envelopes, expected_epoch)?;
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }

            // (3) Record the whole-body recovery only AFTER success, so a later replay/explain returns it
            //     verbatim with no second append.
            let outcomes = outcomes_from_recovery(&recovery);
            if let Some(rid) = request_id {
                let expires_at = request_expires_at(now, retention);
                g.commit_idempotency
                    .entry(shard.clone())
                    .or_default()
                    .record(rid, fingerprint, recovery, expires_at);
            }
            Ok(outcomes)
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// RecoveryReadPort — explain_commit + side_record (Snorri recovery/audit reads)
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> RecoveryReadPort
    for ComposedBackend<L, P, C>
{
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let g = self.inner.lock().expect("poisoned");
        // A backend with no commit boundary exposes no recovery surface (parity with the trait default).
        let result = if !self.is_atomic() || !g.projection.supports_commit_transition() {
            Err(EngineError::Unavailable)
        } else {
            Ok(g.commit_idempotency
                .get(shard)
                .and_then(|c| c.peek(&request_id))
                .map(|entries| CommitRecovery {
                    request_id,
                    entries,
                }))
        };
        std::future::ready(result)
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .projection
            .side_record(shard, key);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Default-impl ports (relational-class features the log-replay composition refuses). These keep
// ComposedBackend wirable into the LibBackend bound; each inherits the `Unavailable` default. Gate state
// (SetGates) and per-group active-scope discovery are relational-only — the in-memory / log-replay family
// stores neither, so it refuses them exactly as the monolithic `MemoryBackend` does (capability parity).
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::SetGatesPort
    for ComposedBackend<L, P, C>
{
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::DiscoveryPort
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

#[cfg(test)]
mod ordered_tests {
    use super::*;
    use crate::port::{ChangeRecordSink, ClaimPort, ControlPlaneStore, ProjectionRead, PushPort};
    use pqueue_core::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
        PriorityTieBreaker, RecurrencePolicy, RetryPolicy, TenantId, WorkerId,
    };
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::task::{Poll, Wake};

    #[derive(Default)]
    struct FakeLogState {
        epoch: u64,
        next_sequence: u64,
        buffered: Vec<CommandEnvelope>,
        sealed_batches: Vec<usize>,
        emission_cursor: Option<CommandPosition>,
    }

    #[derive(Default)]
    struct FakeGroupCommitLog {
        state: Mutex<FakeLogState>,
    }

    impl FakeGroupCommitLog {
        fn sealed_batches(&self) -> Vec<usize> {
            self.state
                .lock()
                .expect("fake log poisoned")
                .sealed_batches
                .clone()
        }

        fn seal_buffered(state: &mut FakeLogState, shard: &QueueKey) -> Vec<CommandPosition> {
            let n = state.buffered.len();
            let positions = (0..n)
                .map(|_| {
                    let p = CommandPosition::new(shard.clone(), state.epoch, state.next_sequence);
                    state.next_sequence += 1;
                    p
                })
                .collect();
            state.buffered.clear();
            state.sealed_batches.push(n);
            positions
        }
    }

    impl LogStore for FakeGroupCommitLog {
        fn ensure_shard(&mut self, _shard: &QueueKey) -> EngineResult<()> {
            Ok(())
        }

        fn current_epoch(&self, _shard: &QueueKey) -> EngineResult<u64> {
            Ok(self.state.lock().expect("fake log poisoned").epoch)
        }

        fn acquire_epoch(&mut self, _shard: &QueueKey) -> EngineResult<u64> {
            let state = self.state.get_mut().expect("fake log poisoned");
            state.epoch += 1;
            Ok(state.epoch)
        }

        fn append(
            &mut self,
            shard: &QueueKey,
            commands: &[CommandEnvelope],
            expected_epoch: u64,
        ) -> EngineResult<Vec<CommandPosition>> {
            let state = self.state.get_mut().expect("fake log poisoned");
            if expected_epoch != state.epoch {
                return Err(EngineError::EpochFenced);
            }
            let positions = (0..commands.len())
                .map(|_| {
                    let p = CommandPosition::new(shard.clone(), state.epoch, state.next_sequence);
                    state.next_sequence += 1;
                    p
                })
                .collect();
            state.sealed_batches.push(commands.len());
            Ok(positions)
        }

        fn read_from(
            &self,
            _shard: &QueueKey,
            _from: Option<CommandPosition>,
            _limit: usize,
        ) -> EngineResult<CommandPage> {
            Ok(CommandPage {
                entries: Vec::new(),
                next: None,
            })
        }

        fn high_water(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
            Ok(None)
        }

        fn set_high_water(
            &mut self,
            _shard: &QueueKey,
            _position: CommandPosition,
        ) -> EngineResult<()> {
            Ok(())
        }

        fn emission_cursor(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
            Ok(self
                .state
                .lock()
                .expect("fake log poisoned")
                .emission_cursor
                .clone())
        }

        fn set_emission_cursor(
            &mut self,
            _shard: &QueueKey,
            position: CommandPosition,
        ) -> EngineResult<()> {
            self.state
                .get_mut()
                .expect("fake log poisoned")
                .emission_cursor = Some(position);
            Ok(())
        }

        fn write_snapshot(
            &mut self,
            shard: &QueueKey,
            position: CommandPosition,
            snapshot: ProjectionSnapshot,
        ) -> EngineResult<SnapshotRef> {
            Ok(SnapshotRef {
                queue: shard.clone(),
                position,
                ref_id: String::from_utf8_lossy(&snapshot.payload).into_owned(),
            })
        }

        fn latest_snapshot(&self, _shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
            Ok(None)
        }

        fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
            Ok(ProjectionSnapshot {
                payload: snapshot_ref.ref_id.clone().into_bytes(),
            })
        }

        fn supports_group_commit(&self) -> bool {
            true
        }

        fn gc_enqueue(
            &self,
            _shard: &QueueKey,
            commands: &[CommandEnvelope],
            expected_epoch: u64,
            _now_ms: i64,
        ) -> EngineResult<Vec<CommandPosition>> {
            let mut state = self.state.lock().expect("fake log poisoned");
            if expected_epoch != state.epoch {
                state.buffered.clear();
                return Err(EngineError::EpochFenced);
            }
            state.buffered.extend_from_slice(commands);
            Ok(Vec::new())
        }

        fn gc_seal(
            &self,
            shard: &QueueKey,
            expected_epoch: u64,
            _now_ms: i64,
        ) -> EngineResult<Vec<CommandPosition>> {
            let mut state = self.state.lock().expect("fake log poisoned");
            if expected_epoch != state.epoch {
                state.buffered.clear();
                return Err(EngineError::EpochFenced);
            }
            if state.buffered.is_empty() {
                return Ok(Vec::new());
            }
            Ok(Self::seal_buffered(&mut state, shard))
        }

        fn gc_flush_due(
            &self,
            shard: &QueueKey,
            expected_epoch: u64,
            now_ms: i64,
        ) -> EngineResult<Vec<CommandPosition>> {
            self.gc_seal(shard, expected_epoch, now_ms)
        }

        fn gc_advance_high_water(
            &self,
            _shard: &QueueKey,
            _position: CommandPosition,
        ) -> EngineResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeProjection {
        state: Mutex<FakeProjectionState>,
    }

    #[derive(Default)]
    struct FakeProjectionState {
        pending: Vec<ItemId>,
        leased: BTreeMap<ItemId, LeaseToken>,
        apply_batches: Vec<Vec<&'static str>>,
    }

    impl FakeProjection {
        fn apply_batches(&self) -> Vec<Vec<&'static str>> {
            self.state
                .lock()
                .expect("fake projection poisoned")
                .apply_batches
                .clone()
        }
    }

    impl ProjectionStore for FakeProjection {
        fn ensure_shard(&mut self, _definition: &QueueDefinition) -> EngineResult<()> {
            Ok(())
        }

        fn apply(
            &mut self,
            positions: &[CommandPosition],
            commands: &[CommandEnvelope],
        ) -> EngineResult<()> {
            assert_eq!(positions.len(), commands.len());
            let state = self.state.get_mut().expect("fake projection poisoned");
            state
                .apply_batches
                .push(commands.iter().map(command_kind).collect());
            for env in commands {
                match &env.command {
                    QueueCommand::Push(push) => {
                        state
                            .pending
                            .extend(push.items.iter().map(|item| item.item_id));
                    }
                    QueueCommand::Claim(claim) => {
                        for id in &claim.item_ids {
                            if let Some(pos) =
                                state.pending.iter().position(|pending| pending == id)
                            {
                                state.pending.remove(pos);
                                state.leased.insert(*id, claim.lease_token.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        fn eligible_candidates(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
            max: usize,
        ) -> EngineResult<Vec<ItemId>> {
            Ok(self
                .state
                .lock()
                .expect("fake projection poisoned")
                .pending
                .iter()
                .copied()
                .take(max)
                .collect())
        }

        fn eligible_candidates_after(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
            after: Option<ItemId>,
            max: usize,
        ) -> EngineResult<Vec<ItemId>> {
            let state = self.state.lock().expect("fake projection poisoned");
            let skip = after
                .and_then(|id| state.pending.iter().position(|pending| *pending == id))
                .map_or(0, |pos| pos + 1);
            Ok(state.pending.iter().skip(skip).copied().take(max).collect())
        }

        fn render_claimed(
            &self,
            _shard: &QueueKey,
            ids: &[ItemId],
        ) -> EngineResult<Vec<ClaimedItem>> {
            let state = self.state.lock().expect("fake projection poisoned");
            Ok(ids
                .iter()
                .filter_map(|id| {
                    state.leased.get(id).map(|token| ClaimedItem {
                        item_id: *id,
                        client_item_key: ClientItemKey::new(id.to_string()).unwrap(),
                        item_version: 1,
                        priority: None,
                        group_key: None,
                        not_before: None,
                        lease_token: Some(token.clone()),
                        lease_expires_at: ts(60),
                        attempt_count: 1,
                        payload: None,
                        fields: BTreeMap::new(),
                        metadata: Metadata::default(),
                        gate_keys: Vec::new(),
                    })
                })
                .collect())
        }

        fn lookup_by_key(
            &self,
            _shard: &QueueKey,
            _client_item_key: &ClientItemKey,
        ) -> EngineResult<Option<ItemId>> {
            Ok(None)
        }

        fn item_state(&self, _shard: &QueueKey, _id: &ItemId) -> EngineResult<Option<ItemState>> {
            Ok(None)
        }

        fn item_version(&self, _shard: &QueueKey, _id: &ItemId) -> EngineResult<Option<u64>> {
            Ok(Some(1))
        }

        fn expired_leases(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
        ) -> EngineResult<Vec<ItemId>> {
            Ok(Vec::new())
        }

        fn all_expired_leases(&self, _now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
            Vec::new()
        }

        fn finalize_validate(
            &self,
            _shard: &QueueKey,
            _outcomes: &[FinalizeOutcome],
        ) -> EngineResult<()> {
            Ok(())
        }

        fn renew_validate(&self, _shard: &QueueKey, _ids: &[ItemId]) -> EngineResult<()> {
            Ok(())
        }

        fn reassign_validate(&self, _shard: &QueueKey, _ids: &[ItemId]) -> EngineResult<()> {
            Ok(())
        }

        fn update_fields_validate(
            &self,
            _shard: &QueueKey,
            _id: &ItemId,
            _expected_item_version: Option<u64>,
        ) -> EngineResult<()> {
            Ok(())
        }

        fn index_validate(
            &self,
            _shard: &QueueKey,
            _item_id: &ItemId,
            _fields: &BTreeMap<String, Bytes>,
            _entity: Option<&serde_json::Value>,
            _exclude: Option<&ItemId>,
        ) -> EngineResult<()> {
            Ok(())
        }

        fn index_validate_push(&self, _shard: &QueueKey, _items: &[PushItem]) -> EngineResult<()> {
            Ok(())
        }

        fn index_validate_replace(
            &self,
            _shard: &QueueKey,
            _existing_id: &ItemId,
            _item: &PushItem,
        ) -> EngineResult<()> {
            Ok(())
        }

        fn index_validate_update(
            &self,
            _shard: &QueueKey,
            _id: &ItemId,
            _field_ops: &BTreeMap<String, Option<Bytes>>,
            _entity: Option<&serde_json::Value>,
        ) -> EngineResult<()> {
            Ok(())
        }

        fn select_eligible(
            &self,
            shard: &QueueKey,
            now: UtcTimestamp,
            limit: usize,
        ) -> EngineResult<Vec<ItemId>> {
            self.eligible_candidates(shard, now, limit)
        }

        fn peek(&self, _shard: &QueueKey, _limit: usize) -> EngineResult<Vec<ItemView>> {
            Ok(Vec::new())
        }

        fn pending(&self, _shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
            Ok(Vec::new())
        }

        fn metrics(&self, _shard: &QueueKey) -> EngineResult<QueueMetrics> {
            let pending = self
                .state
                .lock()
                .expect("fake projection poisoned")
                .pending
                .len() as u64;
            Ok(QueueMetrics {
                pending,
                ..Default::default()
            })
        }

        fn live_items(
            &self,
            _shard: &QueueKey,
            keys: &[ClientItemKey],
        ) -> EngineResult<Vec<Option<LiveItemView>>> {
            Ok(keys.iter().map(|_| None).collect())
        }

        fn grouped_aggregate(
            &self,
            _shard: &QueueKey,
            _request: GroupedAggregateRequest,
        ) -> EngineResult<GroupedAggregateResponse> {
            Err(EngineError::Unavailable)
        }

        fn declared_bucket_segment(
            &self,
            _shard: &QueueKey,
            _request: DeclaredBucketSegmentRequest,
        ) -> EngineResult<DeclaredBucketSegmentResponse> {
            Err(EngineError::Unavailable)
        }

        fn index_get_unique(
            &self,
            _shard: &QueueKey,
            _index: &str,
            _key: &[Vec<u8>],
        ) -> EngineResult<Option<IndexHit>> {
            Ok(None)
        }

        fn index_lookup(
            &self,
            _shard: &QueueKey,
            _index: &str,
            _key: &[Vec<u8>],
        ) -> EngineResult<Vec<IndexHit>> {
            Ok(Vec::new())
        }
    }

    fn command_kind(env: &CommandEnvelope) -> &'static str {
        match env.command {
            QueueCommand::Push(_) => "push",
            QueueCommand::Claim(_) => "claim",
            _ => "other",
        }
    }

    fn queue() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
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
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
        }
    }

    fn ts(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(seconds, 0).unwrap()
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let waker = std::task::Waker::from(Arc::new(NoopWake));
        let mut cx = std::task::Context::from_waker(&waker);
        Pin::new(future).poll(&mut cx)
    }

    #[test]
    fn ordered_hybrid_async_barrier_force_seals_before_claims_and_applies_before_release() {
        let backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        )
        .with_group_commit(true);
        let shard = queue();
        assert!(matches!(
            poll_once(&mut backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));
        let epoch = match poll_once(&mut backend.current_epoch(&shard)) {
            Poll::Ready(Ok(epoch)) => epoch,
            other => panic!("unexpected current_epoch result: {other:?}"),
        };

        let mut first_push = backend.push(&shard, vec![PushSpec::default()], ts(0), Some(epoch));
        let mut second_push = backend.push(&shard, vec![PushSpec::default()], ts(0), Some(epoch));
        assert!(matches!(poll_once(&mut first_push), Poll::Pending));
        assert!(matches!(poll_once(&mut second_push), Poll::Pending));
        assert_eq!(
            backend.with_log(|log| log.sealed_batches()),
            Vec::<usize>::new(),
            "buffered pushes do not create one-command segments"
        );

        let mut first_claim = backend.claim(ClaimRequest {
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer-1").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(epoch),
        });
        assert!(matches!(poll_once(&mut first_claim), Poll::Pending));
        let first_ids = match poll_once(&mut first_push) {
            Poll::Ready(Ok(ids)) => ids,
            other => panic!("first push was not released after force-seal/apply: {other:?}"),
        };
        let second_ids = match poll_once(&mut second_push) {
            Poll::Ready(Ok(ids)) => ids,
            other => panic!("second push was not released after force-seal/apply: {other:?}"),
        };

        let mut second_claim = backend.claim(ClaimRequest {
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer-2").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-2").unwrap(),
            lease_expires_at: ts(60),
            now: ts(2),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(epoch),
        });
        assert!(matches!(poll_once(&mut second_claim), Poll::Pending));

        backend.flush_tick(2_001).expect("flush claims");

        let first_claim = match poll_once(&mut first_claim) {
            Poll::Ready(Ok(claimed)) => claimed,
            other => panic!("unexpected first claim result: {other:?}"),
        };
        assert_eq!(first_claim.items.len(), 1);
        assert_eq!(first_claim.items[0].item_id, first_ids[0]);

        let second_claim = match poll_once(&mut second_claim) {
            Poll::Ready(Ok(claimed)) => claimed,
            other => panic!("unexpected second claim result: {other:?}"),
        };
        assert_eq!(second_claim.items.len(), 1);
        assert_eq!(second_claim.items[0].item_id, second_ids[0]);
        assert_ne!(
            first_claim.items[0].item_id, second_claim.items[0].item_id,
            "the second claim observes the first claim's memory apply"
        );
        assert_eq!(
            poll_once(&mut backend.metrics(&shard)),
            Poll::Ready(Ok(QueueMetrics {
                pending: 0,
                ..Default::default()
            }))
        );
        assert_eq!(
            backend.with_log(|log| log.sealed_batches()),
            vec![2, 2],
            "the two buffered pushes seal as one ordered batch; normal claims batch before acknowledgement"
        );
        assert_eq!(
            backend
                .inner
                .lock()
                .expect("backend poisoned")
                .projection
                .apply_batches(),
            vec![vec!["push", "push"], vec!["claim", "claim"]]
        );
    }

    #[derive(Default)]
    struct DedupSink {
        seen: Mutex<BTreeSet<(TenantId, pqueue_core::QueueId, Option<ItemId>, u64, u64)>>,
    }

    impl crate::port::ChangeRecordSink for DedupSink {
        fn emit(
            &self,
            shard: &QueueKey,
            records: &[crate::command::ChangeRecord],
        ) -> EngineResult<()> {
            let mut seen = self.seen.lock().expect("sink poisoned");
            for record in records {
                seen.insert(record.idempotency_key(shard));
            }
            Ok(())
        }
    }

    #[test]
    fn emission_cursor_at_least_once() {
        let mut log = FakeGroupCommitLog::default();
        let shard = queue();
        let emitted_at = ts(123);
        let epoch = log.acquire_epoch(&shard).expect("epoch");
        let env = CommandEnvelope {
            command_id: CommandId::new("cmd-1"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![ItemId::from_u64(1), ItemId::from_u64(2)],
            command: QueueCommand::Push(PushCommand {
                items: vec![
                    PushItem {
                        client_item_key: pqueue_core::ClientItemKey::new("k-1").unwrap(),
                        item_id: ItemId::from_u64(1),
                        priority: None,
                        not_before: None,
                        group_key: None,
                        max_attempts: 3,
                        payload: None,
                        fields: Default::default(),
                        metadata: Default::default(),
                        cohort_size: None,
                        gate_keys: Vec::new(),
                        entity_document: None,
                    },
                    PushItem {
                        client_item_key: pqueue_core::ClientItemKey::new("k-2").unwrap(),
                        item_id: ItemId::from_u64(2),
                        priority: None,
                        not_before: None,
                        group_key: None,
                        max_attempts: 3,
                        payload: None,
                        fields: Default::default(),
                        metadata: Default::default(),
                        cohort_size: None,
                        gate_keys: Vec::new(),
                        entity_document: None,
                    },
                ],
            }),
            checksum: CommandChecksum(0),
            created_at: emitted_at,
        };
        let positions = log
            .append(&shard, std::slice::from_ref(&env), epoch)
            .expect("append");
        let page_entries = vec![(positions[0].clone(), env.clone())];
        let sink = DedupSink::default();
        let records = page_entries
            .iter()
            .flat_map(|(position, env)| {
                command_envelope_change_records(&shard, position, env, emitted_at, None)
            })
            .collect::<Vec<_>>();
        sink.emit(&shard, &records).expect("first emit");
        let replay_records = page_entries
            .iter()
            .flat_map(|(position, env)| {
                command_envelope_change_records(&shard, position, env, emitted_at, None)
            })
            .collect::<Vec<_>>();
        sink.emit(&shard, &replay_records).expect("second emit");
        log.set_emission_cursor(&shard, positions.last().cloned().expect("position"))
            .expect("advance");
        assert_eq!(
            log.emission_cursor(&shard).unwrap(),
            positions.last().cloned()
        );
        let seen = sink.seen.lock().expect("sink poisoned").clone();
        assert_eq!(seen.len(), 2);
        assert!(seen.contains(&(
            shard.tenant_id.clone(),
            shard.queue_id.clone(),
            Some(ItemId::from_u64(1)),
            epoch,
            positions[0].sequence,
        )));
        assert!(seen.contains(&(
            shard.tenant_id.clone(),
            shard.queue_id.clone(),
            Some(ItemId::from_u64(2)),
            epoch,
            positions[0].sequence,
        )));
    }
}

#[cfg(test)]
mod poison_tests {
    //! Recovery-on-open poison / high-water fail-closed decisions (TD-004 §"Async apply debt,
    //! backpressure, and poison thresholds"; bead pqueue-6da52695).
    use super::*;

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn hw(seq: u64) -> CommandPosition {
        CommandPosition::new(shard(), 0, seq)
    }

    #[test]
    fn poison_stops_recovery_by_failing_closed() {
        // Unresolved replay poison MUST stop serving: recovery resolution returns a Storage error rather
        // than a replay-start position, so the composition aborts recover() instead of hydrating.
        let err = resolve_recovery_start(Some("persistent checkpoint error"), false, Some(hw(41)))
            .expect_err("a poisoned projection must fail closed");
        match err {
            EngineError::Storage(msg) => {
                assert!(msg.contains("poisoned"), "{msg}");
                assert!(msg.contains("persistent checkpoint error"), "{msg}");
            }
            other => panic!("expected Storage poison error, got {other:?}"),
        }
    }

    #[test]
    fn poison_high_water_never_advances_past_the_poison_point() {
        // Even with a recorded high-water present, poison forbids advancing past it — the resolver never
        // yields a FromHighWater(Some(..)) skip point for a poisoned shard.
        let result = resolve_recovery_start(Some("corruption"), false, Some(hw(100)));
        assert!(
            result.is_err(),
            "high-water must not advance past poison: {result:?}"
        );
    }

    #[test]
    fn hard_backpressure_without_poison_replays_from_genesis_not_the_lagging_high_water() {
        // A lagging (hard-backpressured) but un-poisoned projection MUST NOT advertise its high-water as a
        // safe replay-skip point; recovery restarts from genesis so no acknowledged command is skipped.
        let start = resolve_recovery_start(None, true, Some(hw(41)))
            .expect("hard backpressure is repairable, not fail-closed");
        assert_eq!(start, RecoveryStart::FromGenesis);
    }

    #[test]
    fn healthy_projection_trusts_its_recovery_high_water() {
        let start = resolve_recovery_start(None, false, Some(hw(41)))
            .expect("a healthy projection trusts its high-water");
        assert_eq!(start, RecoveryStart::FromHighWater(Some(hw(41))));
    }

    #[test]
    fn healthy_empty_projection_replays_from_genesis() {
        let start = resolve_recovery_start(None, false, None)
            .expect("empty projection replays from genesis");
        assert_eq!(start, RecoveryStart::FromHighWater(None));
    }

    #[test]
    fn poison_takes_precedence_over_hard_backpressure() {
        // When both conditions hold, fail-closed poison wins over the (softer) genesis-replay path.
        assert!(resolve_recovery_start(Some("gap"), true, None).is_err());
    }
}
