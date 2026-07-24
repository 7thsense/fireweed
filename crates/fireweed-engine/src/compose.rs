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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use fireweed_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByQueryRequest, ClientItemKey,
    CohortId, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, ItemId, ItemState, LeaseToken, Metadata,
    MetricsByQueryRequest, OrderingMode, PriorityValue, QueryCapabilityFlags, QueueDefinition,
    QueueId, RangeScanRequest, RangeScanResponse, RequestId, TenantId, UtcTimestamp,
};

use crate::active_scope::{ActiveScope, DiscoveryGranularity};
use crate::claim_validation::{ClaimCompatibility, ClaimUnit, validate_claim_compatibility};
use crate::command::{
    AdvanceInstanceFenceCommand, ClaimCommand, CohortClaimCommand, CommandChecksum,
    CommandEnvelope, CommandId, CommitOutcomeEntry, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, PayloadUpdate, PurgeItemsCommand, PushCommand, PushItem, QueueCommand,
    QueueCounters, ReassignLeaseCommand, RenewLeaseCommand, ReplacePendingCommand, RequestOutcome,
    ScheduleUpdate, SetGatesCommand, UpdateFieldsCommand, WriteSideRecordsCommand,
    build_push_items, command_envelope_change_records, validate_gate_command, validate_gate_push,
    validate_request_replay_metadata,
};
use crate::error::{CommitRejection, EngineError, EngineResult};
use crate::finalize_validation::validate_purge_force;
use crate::idempotency::{IdempotencyDecision, QueueIdempotencyCache};
use crate::maintenance::{
    MaintenanceAuthoritySnapshot, MaintenanceCandidate, MaintenanceDisposition, MaintenanceFilter,
    MaintenanceObjectClass, MaintenancePolicy,
};
use crate::port::{
    AsOfProjectionStore, Backend, ClaimPort, ClaimRef, ClaimRequest, Claimed, ClaimedItem,
    CommandPage, CommitCapabilities, CommitEntryOutcome, CommitEntryStatus, CommitRecovery,
    CommitTransition, CommitTransitionPort, ControlPlaneStore, CreateQueueOutcome, EntryRecovery,
    FinalizePort, HistoricalProjectionRead, IndexHit, IndexQueryPort, ItemView, LeaseView,
    LiveItemView, LogRead, MaintenanceStopReason, MaintenanceSummary, PendingPage, PendingSummary,
    ProjectionRead, ProjectionSnapshot, PurgePort, PushPort, PushSpec, QueueMetrics,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeasePort,
    ReschedulePort, SnapshotRef, SnapshotStore, TerminalEmissionMetrics, TickReport,
    UpdateFieldsPort, UpsertOutcome, UpsertPort, generate_query_lease_token,
    validate_api001_reserved_write_fields, validate_instance_fence,
};
use crate::schema_validation::{compile_entity_schema, validate_entity};
use crate::sequenced_metadata::{AdvanceThenDelete, RetainedAddress, RetentionFloorClass};
use crate::types::{CommandPosition, DurabilityClass, QueueKey};
use crate::{
    BufferedByteBudget, ByteAdmissionError, OwnedBytePermit, retained_records_plus_frame_bytes,
};

/// Defer synchronous compatibility work until the returned future is polled.
///
/// This helper is confined to the legacy in-process composition while each substrate moves behind its
/// native async axis. In particular, constructing a port future must not itself perform storage work.
struct Deferred<F> {
    operation: Option<F>,
}

impl<F> Unpin for Deferred<F> {}

impl<T, F> Future for Deferred<F>
where
    F: FnOnce() -> T,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self
            .operation
            .take()
            .expect("deferred future polled after completion")(
        ))
    }
}

fn deferred<T, F>(operation: F) -> Deferred<F>
where
    T: Send,
    F: FnOnce() -> T + Send,
{
    Deferred {
        operation: Some(operation),
    }
}

struct QueueSerialized<F> {
    acquire: crate::QueueGateAcquire<QueueKey>,
    operation: Option<F>,
}

impl<F> Unpin for QueueSerialized<F> {}

impl<T, F> Future for QueueSerialized<F>
where
    F: FnOnce() -> EngineResult<T>,
{
    type Output = EngineResult<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.acquire).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(EngineError::Unavailable)),
            Poll::Ready(Ok(permit)) => {
                let operation = self
                    .operation
                    .take()
                    .expect("queue-serialized future polled after completion");
                let result = operation();
                drop(permit);
                Poll::Ready(result)
            }
        }
    }
}

fn queue_serialized<T, F>(
    gate: &crate::KeyedQueueGate<QueueKey>,
    shard: QueueKey,
    operation: F,
) -> QueueSerialized<F>
where
    T: Send,
    F: FnOnce() -> EngineResult<T> + Send,
{
    QueueSerialized {
        acquire: gate.acquire(shard),
        operation: Some(operation),
    }
}

/// Opaque, storage-ordered durable queue-catalog cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionCursor {
    pub storage_key: String,
}

/// One bounded durable queue-catalog page. `next` may be present when `definitions` is empty because the
/// storage page contained only queues assigned to other workers.
#[derive(Debug, Clone, PartialEq)]
pub struct DefinitionPage {
    pub definitions: Vec<QueueDefinition>,
    pub next: Option<DefinitionCursor>,
}

const DEFINITION_PAGE_LIMIT: usize = 256;

fn definition_storage_key(definition: &QueueDefinition) -> String {
    serde_json::to_string(&(definition.tenant_id.as_str(), definition.queue_id.as_str()))
        .expect("queue identity serializes")
}

impl DefinitionCursor {
    pub fn from_queue(queue: &QueueKey) -> Self {
        Self {
            storage_key: serde_json::to_string(&(
                queue.tenant_id.as_str(),
                queue.queue_id.as_str(),
            ))
            .expect("queue identity serializes"),
        }
    }

    pub fn queue_parts(&self) -> EngineResult<(String, String)> {
        serde_json::from_str(&self.storage_key)
            .map_err(|error| EngineError::Storage(format!("invalid definition cursor: {error}")))
    }
}

pub fn definition_page_from_storage_rows(
    mut rows: Vec<QueueDefinition>,
    has_more: bool,
    worker_partition: Option<(usize, usize)>,
) -> DefinitionPage {
    let next = has_more.then(|| {
        let last = rows.last().expect("continued page is nonempty");
        DefinitionCursor::from_queue(&QueueKey::new(
            last.tenant_id.clone(),
            last.queue_id.clone(),
        ))
    });
    rows.retain(|definition| {
        worker_partition.is_none_or(|(index, partitions)| {
            queue_worker_partition(
                &QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                partitions,
            ) == index
        })
    });
    DefinitionPage {
        definitions: rows,
        next,
    }
}

pub fn definition_page_from_sorted_rows(
    definitions: impl IntoIterator<Item = QueueDefinition>,
    cursor: Option<&DefinitionCursor>,
    limit: usize,
    worker_partition: Option<(usize, usize)>,
) -> EngineResult<DefinitionPage> {
    if limit == 0 {
        return Err(EngineError::Invalid(
            "definition page limit must be nonzero",
        ));
    }
    let after = cursor.map(DefinitionCursor::queue_parts).transpose()?;
    let mut rows = definitions
        .into_iter()
        .filter(|definition| {
            after.as_ref().is_none_or(|(tenant, queue)| {
                definition.tenant_id.as_str() > tenant.as_str()
                    || (definition.tenant_id.as_str() == tenant.as_str()
                        && definition.queue_id.as_str() > queue.as_str())
            })
        })
        .collect::<Vec<_>>();
    // Relational projection adapters currently keep their recovered definitions in a HashMap.  The
    // iterator order is therefore deliberately unspecified: establish the same global key order as the
    // durable SQL catalog before applying limit+1, otherwise a keyset cursor can skip or duplicate queues.
    rows.sort_unstable_by(|left, right| {
        (&left.tenant_id, &left.queue_id).cmp(&(&right.tenant_id, &right.queue_id))
    });
    rows.truncate(limit.saturating_add(1));
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    Ok(definition_page_from_storage_rows(
        rows,
        has_more,
        worker_partition,
    ))
}

/// Opaque keyset cursor for one bounded global expired-lease sweep page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredLeaseCursor {
    storage_key: String,
}

impl ExpiredLeaseCursor {
    pub fn from_row(lease_expires_at_nanos: i64, queue: &QueueKey, item_id: &ItemId) -> Self {
        Self {
            storage_key: serde_json::to_string(&(
                lease_expires_at_nanos,
                queue.tenant_id.as_str(),
                queue.queue_id.as_str(),
                item_id.to_string(),
            ))
            .expect("expired lease cursor serializes"),
        }
    }

    pub fn row_parts(&self) -> EngineResult<(i64, String, String, String)> {
        serde_json::from_str(&self.storage_key)
            .map_err(|error| EngineError::Storage(format!("invalid expired lease cursor: {error}")))
    }
}

/// One raw-storage-bounded expired-lease page. A partition may legitimately receive an empty page with a
/// continuation cursor when the raw page belongs entirely to other workers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExpiredLeasePage {
    pub leases: Vec<(QueueKey, Vec<ItemId>)>,
    pub next: Option<ExpiredLeaseCursor>,
}

/// Bounded fallback for in-process adapters. Durable adapters override the page method with storage-level
/// keyset pagination; this helper preserves the same cursor/partition contract for tests and memory stores.
fn definition_page_from_iter(
    definitions: impl IntoIterator<Item = QueueDefinition>,
    cursor: Option<&DefinitionCursor>,
    limit: usize,
    worker_partition: Option<(usize, usize)>,
) -> EngineResult<DefinitionPage> {
    if limit == 0 {
        return Err(EngineError::Invalid(
            "definition page limit must be nonzero",
        ));
    }
    let mut rows: Vec<_> = definitions
        .into_iter()
        .map(|definition| (definition_storage_key(&definition), definition))
        .collect();
    rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let after = cursor.map(|cursor| cursor.storage_key.as_str());
    let mut raw = rows
        .into_iter()
        .filter(|(key, _)| after.is_none_or(|after| key.as_str() > after))
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let has_more = raw.len() > limit;
    raw.truncate(limit);
    let next = has_more.then(|| DefinitionCursor {
        storage_key: raw.last().expect("nonzero bounded page").0.clone(),
    });
    let definitions = raw
        .into_iter()
        .map(|(_, definition)| definition)
        .filter(|definition| {
            worker_partition.is_none_or(|(index, partitions)| {
                queue_worker_partition(
                    &QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                    partitions,
                ) == index
            })
        })
        .collect();
    Ok(DefinitionPage { definitions, next })
}

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

    /// Epoch this process has positively acquired for serving, if any. Reading durable `current_epoch` is not
    /// ownership proof and therefore cannot authorize background deletion by itself.
    fn maintenance_owner_epoch(&self, _shard: &QueueKey) -> Option<u64> {
        None
    }

    fn supports_objectlog_maintenance(&self) -> bool {
        false
    }

    /// Clone an owned maintenance handle whose provider I/O does not borrow the composed unit-of-work.
    ///
    /// The normal log methods intentionally run while `ComposedBackend` holds its atomic append/apply lock.
    /// Object-log retention is different: bounded LIST/GET/DELETE and manifest-CAS calls may wait on a remote
    /// provider and therefore must execute after that lock is released. Implementations expose a handle only
    /// when the underlying substrate is independently shared and every destructive operation is owner-fenced.
    fn detached_maintenance(&self) -> Option<Arc<dyn DetachedLogMaintenance>> {
        None
    }

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

    /// Append using canonical bytes prepared by the composition's pre-ownership admission boundary. Logs
    /// that retain encoded records override this to consume them; other axes preserve existing behavior.
    fn append_serialized(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        serialized: Vec<Vec<u8>>,
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        drop(serialized);
        self.append(shard, commands, expected_epoch)
    }

    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> EngineResult<CommandPage>;

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>>;
    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()>;

    // -- retention floor (bounded-recovery segment-object reclamation, bead pqueue-b5cc2bc7) -------------
    //
    // The durable retention floor + segment-object trim seam. ALL default to no-op / `Ok(None)` / `Ok(0)`, so
    // every non-object-log backend (memory, sqlite-log, relational, postgres) is UNAFFECTED — only the
    // segmented object log overrides them. The composition computes a trim horizon (min of the durable
    // checkpoint high-water and the request-id-retention-expired manifest prefix), writes the floor FIRST,
    // then deletes the segment objects — the crash-safe order that never leaves the floor pointing past a
    // deleted segment. See `trim_reclaimable_segments_locked`.

    /// The durably-recorded retention floor: the highest command position whose segment objects have been
    /// trimmed, an EXCLUSIVE lower bound (recovery/idempotency folds resume at `sequence + 1`). `None` (the
    /// default, and a never-trimmed log) means genesis — folds start from the beginning, byte-identical to a
    /// pre-floor log.
    fn retention_floor(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        Ok(None)
    }

    /// Monotonically advance the durable retention floor to `position` (rejecting a regression). Written
    /// BEFORE bounded segment expiry deletes the corresponding segment objects. `expected_epoch` is the
    /// writing owner's currently-held assignment epoch; the impl re-reads the authoritative current epoch and
    /// rejects a SUPERSEDED writer with [`EngineError::EpochFenced`] (bug 2b — a stale owner must not lower a
    /// newer owner's floor). Default: no-op.
    fn advance_retention_floor(
        &mut self,
        _shard: &QueueKey,
        _position: CommandPosition,
        _expected_epoch: u64,
    ) -> EngineResult<()> {
        Ok(())
    }

    /// The highest command sequence whose segment is safe to trim by REQUEST-ID RETENTION: the max
    /// `visible_last_seq` over the contiguous prefix of data segments committed at or before `cutoff_ms`. The
    /// composition takes the min of this and the durable checkpoint high-water to get the trim horizon.
    /// Default: `None` — a non-segmented log has nothing to trim.
    fn max_trimmable_seq_before(
        &self,
        _shard: &QueueKey,
        _cutoff_ms: i64,
    ) -> EngineResult<Option<u64>> {
        Ok(None)
    }

    /// Run one bounded deletion pass over segment objects at or before `through_seq`, keeping their manifest
    /// entries as tombstones and skipping branch-pinned segments. The summary preserves partial progress,
    /// resource accounting, cursor state, and typed stop reasons. Default: completed no-op.
    fn expire_segments_through_bounded(
        &mut self,
        _shard: &QueueKey,
        _through_seq: u64,
        _now_ms: i64,
    ) -> EngineResult<MaintenanceSummary> {
        Ok(MaintenanceSummary::default())
    }

    /// Legacy diagnostic surface for locating a branch-pinned segment. Bounded expiry must carry its own
    /// paged pin proof; the composed trim path does not invoke this unbounded compatibility query.
    fn lowest_branch_pinned_below(
        &self,
        _shard: &QueueKey,
        _through_seq: u64,
        _now_ms: i64,
    ) -> EngineResult<Option<u64>> {
        Ok(None)
    }

    /// Run one owner-fenced, bounded orphan-branch maintenance page. Object-log implementations reclassify
    /// and delete under their create/GC exclusion guard; other log families have no branch objects.
    fn gc_orphaned_branches_bounded(
        &mut self,
        _shard: &QueueKey,
        _expected_epoch: u64,
        _now_ms: i64,
    ) -> EngineResult<MaintenanceSummary> {
        Ok(MaintenanceSummary::default())
    }

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

    /// Whether the log axis persists a durable change-record emission cursor. If false, the
    /// change-record emitter must stay disabled: otherwise a restart can re-read from genesis.
    fn supports_emission_cursor(&self) -> bool {
        false
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

    /// Atomically create or read a durable queue definition when the log catalog, rather than the control
    /// plane, is authoritative across independently opened handles. `None` keeps the existing control-plane
    /// outcome; durable catalog adapters return the decoded stored winner and whether this call inserted it.
    fn create_or_read_definition(
        &mut self,
        definition: &QueueDefinition,
    ) -> EngineResult<Option<CreateQueueOutcome>> {
        self.persist_definition(definition)?;
        Ok(None)
    }

    /// Enumerate the durable queue definitions this log persists, for recovery-on-open (ADR-012 P2). Default:
    /// empty — a reopened in-process log is a fresh process with nothing to recover.
    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(Vec::new())
    }

    /// Read one bounded page of the durable queue catalog. The cursor advances over the underlying
    /// catalog, including definitions owned by other worker partitions, so an empty returned page can still
    /// carry progress. Durable adapters override this to issue a keyset/LIST-page read instead of allocating
    /// the complete catalog.
    fn recover_definitions_page(
        &self,
        cursor: Option<&DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<DefinitionPage> {
        definition_page_from_iter(self.recover_definitions()?, cursor, limit, worker_partition)
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

    fn gc_enqueue_serialized(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        serialized: Vec<Vec<u8>>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        drop(serialized);
        self.gc_enqueue(shard, commands, expected_epoch, now_ms)
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

/// Immutable authority captured while the composition lock and the queue-local mutation permit are held.
/// The owned log handle executes this request after the global composition lock is released; the queue permit
/// remains held, so a new local claim replay cannot appear between preparation and the floor CAS.
#[derive(Debug, Clone)]
pub struct DetachedRetentionRequest {
    pub shard: QueueKey,
    pub expected_epoch: u64,
    pub now_ms: i64,
    pub cutoff_ms: i64,
    pub checkpoint: Option<CommandPosition>,
    pub allow_floor_advance: bool,
    pub completed_through: Option<u64>,
}

/// How a completed detached pass updates the process-local crash-recovery scan watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachedTrimWatermark {
    Unchanged,
    Clear,
    Set(u64),
}

#[derive(Debug, Clone)]
pub struct DetachedRetentionOutcome {
    pub expected_epoch: u64,
    pub summary: MaintenanceSummary,
    pub watermark: DetachedTrimWatermark,
}

/// An owned, epoch-fenced object-log maintenance substrate.
///
/// Implementations must re-read durable owner authority before destructive provider calls. A successful floor
/// publication is the crash barrier before segment deletion; stale/raced owners return a fenced/empty outcome.
pub trait DetachedLogMaintenance: Send + Sync {
    fn execute_retention(
        &self,
        request: DetachedRetentionRequest,
    ) -> EngineResult<DetachedRetentionOutcome>;

    fn execute_orphan_gc(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<MaintenanceSummary>;
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

/// Per-queue group-commit coordination (ADR-012 P2): the engine-side mirror of the substrate's buffer. Held
/// under the composition's existing `std::sync::Mutex<Inner>` — no async lock. `pending` mirrors the
/// substrate's buffered envelopes 1:1 in arrival order (so a seal that drains the substrate buffer drains
/// exactly the same `pending`/`waiters` prefix); each `waiters` slot is filled when its batch seals + applies.
#[derive(Default)]
struct ShardCoord {
    /// Envelopes buffered-but-not-yet-acked, kept engine-side so `distribute` can apply them to the
    /// projection on seal (the substrate's seal returns only positions).
    pending: Vec<CommandEnvelope>,
    permits: Vec<Option<OwnedBytePermit>>,
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

fn map_composed_byte_admission_error(error: ByteAdmissionError) -> EngineError {
    match error {
        ByteAdmissionError::Closed => EngineError::Unavailable,
        ByteAdmissionError::Backpressure => EngineError::Backpressure {
            resource: "buffered bytes",
        },
        ByteAdmissionError::Oversize {
            requested, limit, ..
        } => EngineError::RequestTooLarge { requested, limit },
    }
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

/// The greater of two optional replay-start positions by `(epoch, sequence)`, treating `None` as genesis (the
/// least). Used to floor the recovery replay start at the durable retention floor (bead pqueue-b5cc2bc7): a
/// `FromGenesis` (None) start under Hard backpressure is lifted to the floor so recovery never reads a trimmed
/// below-floor segment, while a healthy checkpoint start (>= floor) is left unchanged.
pub fn max_position(
    a: Option<CommandPosition>,
    b: Option<CommandPosition>,
) -> Option<CommandPosition> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if y.precedes(&x) { x } else { y }),
        (Some(x), None) => Some(x),
        (None, b) => b,
    }
}

/// The result of a NON-item claim selection ([`ProjectionStore::select_rich_claim`]): the candidate item
/// ids to lease, plus the selected cohort id when the unit was `whole_cohort` (so the composition can emit a
/// [`QueueCommand::CohortClaim`] that updates the leased cohort state, and stamp the cohort id on the reply).
#[derive(Debug, Clone, Default)]
pub struct RichClaimSelection {
    /// The item ids to lease. Empty = nothing eligible for this unit (claim nothing).
    pub item_ids: Vec<ItemId>,
    /// `Some` only for a `whole_cohort` selection: the cohort generation being leased.
    pub cohort_id: Option<CohortId>,
}

/// The projection axis: the materialized read model. Exposes the full `ProjectionRead` surface, the
/// secondary-index queries, the pre-commit VALIDATION helpers the orchestration relies on (so the
/// post-append `apply` is infallible — commit has no rollback), and the `apply` seam itself.
///
/// All reads/validation are `&self`; `apply`/`ensure_shard` are `&mut self`. The composition calls these
/// under its UoW lock, so a claim's `select → append → apply → render` is one atomic unit.
pub trait ProjectionStore: Send {
    /// Hot-query capabilities actually implemented by this projection axis.
    ///
    /// Composed backends must report projection behavior, not capabilities of the generic
    /// orchestration layer. The safe default keeps projections that inherit the `Unavailable`
    /// query methods from advertising a surface they cannot serve.
    fn hot_projection_capabilities(&self) -> QueryCapabilityFlags {
        QueryCapabilityFlags::default()
    }

    /// Materialize a shard's projection from its [`QueueDefinition`] (called from `create_queue`).
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()>;

    /// Apply committed `commands` (at `positions`) to the projection.
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

    /// Install a newly discovered shard from one complete, already-buffered durable replay.
    ///
    /// This is the create-loser publication boundary: implementations must either replace the shard from a
    /// scratch image atomically or use a transactional/infallible recovery apply. Returning an error must
    /// leave the previously serving image unchanged. This method deliberately has no default: every
    /// projection family must make its atomicity argument explicit rather than silently inheriting a
    /// potentially partial `apply_recovery` implementation.
    fn install_recovery_shard(
        &mut self,
        _definition: &QueueDefinition,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()>;

    /// Whether `shard` is in the intake-blocking pause mode. The default projection family does not
    /// track intake blocking and therefore reports `false`.
    fn pause_blocks_intake(&self, _shard: &QueueKey) -> EngineResult<bool> {
        Ok(false)
    }

    /// Admission gate consulted by the composition BEFORE a new-work mutating command is committed to the
    /// durable log (TD-004 "Hard debt threshold"). A projection under HARD async-apply backpressure — its
    /// deferred durable-checkpoint backlog over budget — fails new mutating admission CLOSED here with a
    /// typed retryable error ([`EngineError::Unavailable`]) or a `Storage` poison error, so no further
    /// new-work command is enqueued/sealed/acked while the backlog is at risk of an SLO violation. Called on
    /// the pre-commit path with the unit-of-work lock held, so a rejection leaves NO durable effect.
    ///
    /// Every newly appended mutation is gated at the shared append/group-buffer boundary. Idempotent replays
    /// that return without appending remain available, but push, claim, finalize, renew, reassign, purge,
    /// upsert/update, and future mutation kinds all fail closed while the projection is offline, poisoned, or
    /// under Hard async debt. Default: always admit — only guarded projection profiles override this.
    fn admit_mutation(&mut self, _shard: &QueueKey) -> EngineResult<()> {
        Ok(())
    }

    /// Whether segment expiry / terminal-item retention advancement is currently allowed for `shard`
    /// (TD-004 "Retention backpressure"): retention MUST stop advancing while async-apply debt is over
    /// budget, lineage validation is incomplete, or the async SQLite worker is poisoned — a lagging local
    /// `sqlite_high_water` alone never authorizes deletion. The composition consults this on the real
    /// terminal-reap path and withholds reaping when it returns `false`. Default: `true` — only the
    /// `objectlog/hybrid-async` projection (with an armed monitor) withholds retention.
    fn retention_may_advance(&self, _shard: &QueueKey) -> bool {
        true
    }

    /// Hybrid-async profiles require the complete TD-004 five-way frontier rather than the legacy
    /// checkpoint/time horizon. Default profiles keep their established safe reclamation behavior.
    fn requires_complete_retention_frontier(&self) -> bool {
        false
    }

    /// Whether all async-only frontier evidence has been assembled from authority. `false` is a conservative
    /// retain, never permission to synthesize missing snapshot/item-key/lineage inputs.
    fn complete_retention_frontier_is_proven(&self, _shard: &QueueKey) -> bool {
        !self.requires_complete_retention_frontier()
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
    fn select_item_claim(
        &self,
        shard: &QueueKey,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        if compatibility.group_key.is_some() || !compatibility.metadata_equals.is_empty() {
            return Err(EngineError::Unavailable);
        }
        self.eligible_candidates(shard, now, max)
    }
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
    /// One bounded keyset page for the global reclaim driver. Durable relational implementations override
    /// this with storage-level pagination; this fallback is for bounded in-process projections.
    fn expired_leases_page(
        &self,
        now: UtcTimestamp,
        cursor: Option<&ExpiredLeaseCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<ExpiredLeasePage> {
        if limit == 0 {
            return Err(EngineError::Invalid(
                "expired lease page limit must be nonzero",
            ));
        }
        let after = cursor.map(ExpiredLeaseCursor::row_parts).transpose()?;
        let mut rows = self
            .all_expired_leases(now)
            .into_iter()
            .flat_map(|(queue, ids)| ids.into_iter().map(move |id| (queue.clone(), id)))
            .collect::<Vec<_>>();
        rows.sort_unstable_by(|(left_queue, left_id), (right_queue, right_id)| {
            (&left_queue.tenant_id, &left_queue.queue_id, left_id).cmp(&(
                &right_queue.tenant_id,
                &right_queue.queue_id,
                right_id,
            ))
        });
        let mut raw = rows
            .into_iter()
            .filter(|(queue, id)| {
                after.as_ref().is_none_or(|(_, tenant, queue_id, item_id)| {
                    (
                        queue.tenant_id.as_str(),
                        queue.queue_id.as_str(),
                        id.to_string(),
                    ) > (tenant.as_str(), queue_id.as_str(), item_id.clone())
                })
            })
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = raw.len() > limit;
        raw.truncate(limit);
        let next = has_more.then(|| {
            let (queue, id) = raw.last().expect("nonzero bounded page");
            ExpiredLeaseCursor::from_row(0, queue, id)
        });
        let mut leases = Vec::<(QueueKey, Vec<ItemId>)>::new();
        for (queue, id) in raw.into_iter().filter(|(queue, _)| {
            worker_partition.is_none_or(|(index, partitions)| {
                queue_worker_partition(queue, partitions) == index
            })
        }) {
            match leases.last_mut() {
                Some((last, ids)) if *last == queue => ids.push(id),
                _ => leases.push((queue, vec![id])),
            }
        }
        Ok(ExpiredLeasePage { leases, next })
    }

    // -- rich (non-item) claim selection + relational-class capabilities (BQ-14) ---------------------
    //
    // These back the composition's non-item claim path (whole-group / same-group-key / whole-cohort), the
    // operator gate-state capability, and per-group active-scope discovery. They are RELATIONAL-class: the
    // in-memory / log-replay projection family maintains no per-group summary or cohort/gate tables, so it
    // inherits the `false` / `Unavailable` defaults and the composition refuses these units — exact
    // capability parity with the monolithic `MemoryBackend`. The unified sqlite-relational projection store
    // overrides them by porting its own `select_*` / `discover_active_scopes_sql` SQL.

    /// Whether this projection stores gate membership + gate-state and enforces `SetGates` at claim
    /// selection. `false` (the default) makes the composition refuse gate-bearing pushes and `SetGates`
    /// (the log-replay family has no gate tables); the relational projection overrides it to `true`.
    fn supports_gates(&self) -> bool {
        false
    }

    /// Select the candidates (and, for whole-cohort, the selected cohort id) to lease for a NON-item claim
    /// `unit` (BQ-14b/c). Called under the composition's unit-of-work lock BEFORE the lease command is
    /// committed, so select+lease is one atomic unit (the composition serializes; the relational store
    /// serializes on its own `Mutex<Inner>`). An empty selection means "nothing eligible" (claim nothing).
    /// The default refuses with [`EngineError::Unavailable`] — the log-replay family has no group/cohort
    /// projection to select from, so it rejects non-item units rather than silently downgrading them.
    fn select_rich_claim(
        &self,
        _shard: &QueueKey,
        _unit: ClaimUnit,
        _compatibility: &ClaimCompatibility,
        _now: UtcTimestamp,
        _max_items: usize,
    ) -> EngineResult<RichClaimSelection> {
        Err(EngineError::Unavailable)
    }

    /// Roll up the per-group active-scope summary into ranked [`ActiveScope`]s at `granularity` (BQ-14e).
    /// The default refuses with [`EngineError::Unavailable`] (the log-replay family maintains no per-group
    /// summary); the relational projection overrides it with the `pqueue_group_summary` rollup.
    fn discover_active_scopes(
        &self,
        _shard: &QueueKey,
        _granularity: DiscoveryGranularity,
        _now: UtcTimestamp,
    ) -> EngineResult<Vec<ActiveScope>> {
        Err(EngineError::Unavailable)
    }

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
    fn pending_summary(&self, shard: &QueueKey) -> EngineResult<PendingSummary> {
        Ok(crate::port::summarize_pending(self.pending(shard)?))
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        Ok(crate::port::page_pending(
            self.pending(shard)?,
            start,
            limit,
        ))
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        let start = start.map(|id| id.as_u64()).unwrap_or(0);
        let end = end.map(|id| id.as_u64()).unwrap_or(u64::MAX);
        let mut leases = self.pending(shard)?;
        leases.sort_by_key(|lease| lease.item_id);
        Ok(leases
            .into_iter()
            .filter(|lease| {
                (start..=end).contains(&lease.item_id.as_u64())
                    && consumer.is_none_or(|token| token == &lease.lease_token)
            })
            .take(limit)
            .collect())
    }
    fn pending_by_ids(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<LeaseView>> {
        let by_id: std::collections::HashMap<_, _> = self
            .pending(shard)?
            .into_iter()
            .map(|lease| (lease.item_id, lease))
            .collect();
        Ok(ids.iter().filter_map(|id| by_id.get(id).cloned()).collect())
    }
    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics>;
    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<TerminalEmissionMetrics>;
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>>;

    /// Reap terminal items that are now durable-emission safe for one shard. Projections that do not
    /// materialize terminal-item deletion can keep the default no-op.
    fn reap_terminal_items(
        &mut self,
        _shard: &QueueKey,
        _now: UtcTimestamp,
        _terminal_retention_ms: u64,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(Vec::new())
    }

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

    fn metrics_by_query(
        &self,
        _shard: &QueueKey,
        _request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
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

    /// Read the durable item-id mint ceiling without publishing it into this composition's live counters.
    /// Create-loser hydration uses this prepare-only seam so every fallible projection read completes before
    /// the replacement shard is installed; publication afterward is an infallible [`QueueCounters::observe`].
    fn recovery_counter_high_water(&self, _shard: &QueueKey) -> EngineResult<Option<ItemId>> {
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

    /// Projection-axis counterpart to [`LogStore::recover_definitions_page`].
    fn recover_definitions_page(
        &self,
        cursor: Option<&DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<DefinitionPage> {
        definition_page_from_iter(self.recover_definitions()?, cursor, limit, worker_partition)
    }

    /// Seed the composition's per-queue id-mint `counters` past every item id already materialized in the
    /// durable projection snapshot, so a push after a snapshot-tail reopen never re-mints an existing id.
    /// Default: no-op — the in-memory projection has no persisted snapshot, so its counters are restored by
    /// observing the ids in the replayed log instead.
    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        if let Some(item_id) = self.recovery_counter_high_water(shard)? {
            counters.observe(shard, item_id);
        }
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

    /// Install a definition already selected by a separate durable authority. Control planes that are
    /// themselves authoritative never call this seam; the in-process cache uses it for durable log catalogs.
    fn cache_authoritative_definition(&self, _definition: QueueDefinition) -> EngineResult<()> {
        Ok(())
    }
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

    fn cache_authoritative_definition(&self, definition: QueueDefinition) -> EngineResult<()> {
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.queues
            .lock()
            .expect("poisoned")
            .insert(key, definition);
        Ok(())
    }
}

impl crate::AsyncControlPlane for InProcessControlPlane {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        deferred(move || ControlPlane::create_queue(self, definition))
    }

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        deferred(move || ControlPlane::queue_definition(self, &key))
    }

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        deferred(move || ControlPlane::list_queues(self, &tenant))
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
    claim_by_query_idempotency: HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken)>>,
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
    /// Queue keys known to this composition, used to pre-bind per-queue admission for the raw UoW writer
    /// before Rust's disjoint log/projection borrows are handed to the closure.
    known_shards: HashSet<QueueKey>,
    /// Per-queue IN-MEMORY watermark of the highest sequence whose below-floor segment objects this process
    /// has already fully deleted (bead pqueue-b5cc2bc7). Empty on process start, so the FIRST trim tick after
    /// a (re)open re-runs bounded expiry up to the durable floor to FINISH any deletion a crash
    /// interrupted BETWEEN the floor write and the segment delete — the deletion is idempotent, and this
    /// watermark keeps subsequent idle ticks from re-scanning the manifest once the durable floor is fully
    /// reclaimed. NOT durable: a restart re-verifies against the durable floor.
    trim_completed_through: HashMap<QueueKey, u64>,
    /// Persisted across maintenance ticks so one tick performs one bounded durable-catalog page instead of
    /// serially walking every queue. Each pooled backend owns an independent cursor and therefore advances
    /// its partition concurrently with the other fixed workers.
    maintenance_catalog_projection: bool,
    maintenance_definition_cursor: Option<DefinitionCursor>,
    expired_lease_cursor: Option<ExpiredLeaseCursor>,
    byte_budget: Option<BufferedByteBudget>,
    queue_byte_limit: Option<usize>,
}

/// Default recovery-window budget: the max durable-log tail (commands) a normal reopen replays beyond the
/// projection's recovery high-water before [`ComposedBackend::recover`] logs a recovery-window warning. The
/// durable projection advances its high-water inside the same transaction that applies each batch, so the
/// tail is normally a handful of commands; exceeding this suggests a projection that has fallen far behind
/// the log. (For a fresh in-memory projection the whole log is the "tail", so the budget is generous.)
pub const DEFAULT_RECOVERY_MAX_TAIL: u64 = 1_000_000;
const RECOVERY_READ_PAGE_LIMIT: usize = 8_192;

struct CommitRecoveryAccum {
    fingerprint: u64,
    created_at: UtcTimestamp,
    pending_side_keys: Vec<Vec<u8>>,
    pending_instance: Option<(Vec<u8>, u64)>,
    pending_lifecycle_ids: Vec<ItemId>,
    entries: Vec<EntryRecovery>,
    durable_full: Option<Vec<EntryRecovery>>,
}

struct RecoveryIdempotencyCaches<'a> {
    push: &'a mut HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
    claim: &'a mut HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken)>>,
    commit: &'a mut HashMap<QueueKey, QueueIdempotencyCache<Vec<EntryRecovery>>>,
}

/// A conservative cross-owner clock-skew guard band (ms) subtracted from the retention cutoff before a
/// segment is eligible for object-log trimming (bead pqueue-b5cc2bc7, risk R4): a segment is trimmed only if
/// its `committed_at_ms <= now - request_id_retention_ms - RETENTION_TRIM_SKEW_MARGIN_MS`, so a small clock
/// skew between the sealing owner and the trimming owner can never trim a segment still within retention.
const RETENTION_TRIM_SKEW_MARGIN_MS: i64 = 5_000;

/// The two composed-layer projection-apply crash instants (TP-003 §3.10 AC-TXN-4, row 209). These live in
/// the [`ComposedBackend`] apply step — ABOVE the [`LogStore`] substrate, whose own internal cut points
/// ([`crate`]-external `fireweed_objectlog::FaultCutPoint`) cannot reach them — so they need this seam.
///
/// A commit's durable append has already returned `Ok` by the time the
/// projection apply runs; these instants strike that apply:
///
/// * [`ComposeFaultPoint::DuringProjectionApply`] — while applying the committed command to the projection,
///   BEFORE the projection durably advances (for an eventual-apply log-replay projection the durable state is
///   the log itself, so recovery rebuilds the projection from the durable tail).
/// * [`ComposeFaultPoint::AfterApplyBeforeResponse`] — the projection has applied + durably advanced, but the
///   caller has not yet received its success response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComposeFaultPoint {
    /// Fault while applying the committed command to the projection, before it durably advances.
    DuringProjectionApply,
    /// Fault after the projection has applied + durably advanced, before the caller's response.
    AfterApplyBeforeResponse,
}

/// A TEST-ONLY composed-layer fault hook (TP-003 §3.10 AC-TXN-4): called at each [`ComposeFaultPoint`] the
/// [`ComposedBackend`] projection-apply step passes through. Returning `Err` simulates a process death at that
/// instant (the in-flight unit of work aborts there); `Ok(())` (the default when no hook is installed) lets
/// the apply run normally. The analogue of `fireweed_objectlog::FaultHook` on the composed-apply boundary.
pub trait ComposeFaultHook: Send + Sync {
    fn fault_point(&self, cut: ComposeFaultPoint) -> EngineResult<()>;
}

/// The one generic backend (ADR-012): `Backend = LogStore × ProjectionStore × ControlPlane`. Implements
/// every engine port by delegating to the three axes.
pub struct ComposedBackend<L, P, C> {
    inner: Mutex<Inner<L, P>>,
    control: C,
    /// Independently owned object-log substrate used only for fenced bounded maintenance. Keeping it outside
    /// `Inner` is what lets remote provider I/O run without holding the global append/apply mutex.
    detached_maintenance: Option<Arc<dyn DetachedLogMaintenance>>,
    /// Cancellation-safe queue-local admission. A permit never contains a standard mutex guard.
    mutation_gate: crate::KeyedQueueGate<QueueKey>,
    /// Test-only composed-layer projection-apply fault hook (TP-003 §3.10 AC-TXN-4). `None` in every
    /// production path — installed only by the AC-TXN-4 conformance tests via [`Self::set_fault_hook`].
    fault_hook: Mutex<Option<Arc<dyn ComposeFaultHook>>>,
    /// Packed into every minted [`ItemId`] (ADR-009) so concurrent writers never collide. `0` default.
    node_id: u8,
    counters: QueueCounters,
    /// The durability class inherited from the log axis at assembly (TD-007 §2). Read once from
    /// `LogStore::durability_class` so the hot path never re-locks to decide whether an atomic-only port
    /// (upsert / update_fields / reschedule / commit_transition) is available.
    durability: DurabilityClass,
    supports_gates: bool,
    supports_commit_transition: bool,
    supports_group_commit: bool,
    group_commit_flush_interval_ms: u64,
    /// Recovery-window budget (max tail commands) before [`Self::recover`] logs a recovery-window warning.
    recovery_max_tail: u64,
    /// Group-commit mode (ADR-012 P2), DEFAULT OFF. When `false` every write funnels through the synchronous
    /// `commit_locked` force-seal/append→apply path UNCHANGED. When `true` AND the log axis advertises
    /// `supports_group_commit()`, `push` co-buffers + acks-after-seal and read-modify-write ops force-seal the
    /// buffered batch before they select/apply (so they observe applied state under the one composed lock).
    group_commit: bool,
    worker_partition: Option<(usize, usize)>,
}

/// Stable queue affinity for fixed-size composed worker pools.
pub fn queue_worker_partition(queue: &QueueKey, partitions: usize) -> usize {
    assert!(
        partitions > 0,
        "queue worker partition count must be nonzero"
    );
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    queue.hash(&mut hasher);
    (hasher.finish() as usize) % partitions
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ComposedBackend<L, P, C> {
    /// Reject data-plane work until this handle atomically publishes the shard's serving image. Call while
    /// holding `inner`, before counters, command ids, caches, force-seals, or projection effects.
    fn require_known_shard(inner: &Inner<L, P>, shard: &QueueKey) -> EngineResult<()> {
        if inner.known_shards.contains(shard) {
            Ok(())
        } else {
            Err(EngineError::NotFound)
        }
    }

    /// Assemble a backend from one of each axis.
    pub fn new(log: L, projection: P, control: C) -> Self {
        let durability = log.durability_class();
        let supports_gates = projection.supports_gates();
        let supports_commit_transition = projection.supports_commit_transition();
        let supports_group_commit = log.supports_group_commit();
        let group_commit_flush_interval_ms = (log.gc_max_latency_ms() / 4).max(1);
        let detached_maintenance = log.detached_maintenance();
        Self {
            inner: Mutex::new(Inner {
                log,
                projection,
                idempotency: HashMap::new(),
                claim_by_query_idempotency: HashMap::new(),
                commit_idempotency: HashMap::new(),
                cmd_seq: 0,
                coords: HashMap::new(),
                known_shards: HashSet::new(),
                trim_completed_through: HashMap::new(),
                maintenance_catalog_projection: true,
                maintenance_definition_cursor: None,
                expired_lease_cursor: None,
                byte_budget: None,
                queue_byte_limit: None,
            }),
            control,
            detached_maintenance,
            mutation_gate: crate::KeyedQueueGate::new(1024),
            fault_hook: Mutex::new(None),
            node_id: 0,
            counters: QueueCounters::default(),
            durability,
            supports_gates,
            supports_commit_transition,
            supports_group_commit,
            group_commit_flush_interval_ms,
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            group_commit: false,
            worker_partition: None,
        }
    }

    /// Assign this instance one member of a fixed worker pool. Queue-local calls are routed by the outer
    /// dispatcher; this affinity also prevents node-wide maintenance from processing recovered queues on
    /// more than one connection.
    pub fn with_worker_partition(mut self, index: usize, partitions: usize) -> Self {
        assert!(partitions > 0 && index < partitions);
        self.worker_partition = Some((index, partitions));
        self
    }

    /// Recover one member of a fixed worker pool. Installing affinity before catalog enumeration prevents
    /// every connection from redundantly rebuilding every durable queue.
    pub fn recover_worker_partition(
        mut self,
        index: usize,
        partitions: usize,
    ) -> EngineResult<Self> {
        assert!(partitions > 0 && index < partitions);
        self.worker_partition = Some((index, partitions));
        self.recover()
    }

    fn owns_worker_queue(&self, queue: &QueueKey) -> bool {
        self.worker_partition
            .is_none_or(|(index, count)| queue_worker_partition(queue, count) == index)
    }

    /// Install (or clear, with `None`) a TEST-ONLY composed-layer fault hook that strikes the two
    /// projection-apply instants (TP-003 §3.10 AC-TXN-4, [`ComposeFaultPoint`]). Never called from a
    /// production code path — the hook field defaults to `None`, so the apply path is inert (a `None`-valued
    /// hook clone + two `is_some` checks per write, no behavioral effect) unless a test installs a hook. The
    /// analogue of `fireweed_objectlog::SegmentedObjectLog::set_fault_hook` on the composed-apply boundary.
    pub fn set_fault_hook(&self, hook: Option<Arc<dyn ComposeFaultHook>>) {
        *self.fault_hook.lock().expect("compose fault hook poisoned") = hook;
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

    /// Enable finite resident-byte admission at the full append→durable seal/CAS→projection/apply boundary.
    pub fn with_byte_admission(self, budget: BufferedByteBudget, queue_byte_limit: usize) -> Self {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.byte_budget = Some(budget);
        inner.queue_byte_limit = Some(queue_byte_limit);
        drop(inner);
        self
    }

    pub fn byte_admission_stats(&self) -> Option<crate::BufferedByteBudgetStats> {
        self.inner
            .lock()
            .expect("poisoned")
            .byte_budget
            .as_ref()
            .map(BufferedByteBudget::stats)
    }

    /// Low-cardinality configured limits for production telemetry: `(global, uniform tenant, queue)`.
    pub fn byte_admission_limits(&self) -> Option<(usize, Option<usize>, usize)> {
        let inner = self.inner.lock().expect("poisoned");
        let budget = inner.byte_budget.as_ref()?;
        Some((
            budget.config().global_limit(),
            budget.config().tenant_limit(),
            inner.queue_byte_limit?,
        ))
    }

    /// Whether the composition runs the group-commit write path (the builder flag AND a group-commit-capable
    /// log). The server uses this to decide whether to spawn the externalized flush task.
    pub fn group_commit_enabled(&self) -> bool {
        self.group_commit && self.supports_group_commit
    }

    /// The flush-task poll interval (ms): `gc_max_latency_ms()/4` (≥ 1), so a buffered-but-quiet segment
    /// seals within ~one latency window — the same cadence the monolith's `spawn_flusher` uses.
    pub fn group_commit_flush_interval_ms(&self) -> u64 {
        self.group_commit_flush_interval_ms
    }

    /// Observability/test seam: number of accepted commands still buffered ahead of a durable seal.
    pub fn buffered_group_commit_commands(&self) -> usize {
        self.inner
            .lock()
            .expect("poisoned")
            .coords
            .values()
            .map(|coordinator| coordinator.pending.len())
            .sum()
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

    /// Run lifecycle/repair work against the log and mutable projection under the composition's full
    /// unit-of-work lock. This is intentionally narrower than exposing either axis: callers can reconcile a
    /// disposable projection from authoritative log history without allowing a live append/apply operation
    /// to interleave between reset, replay, and verification.
    pub fn with_log_and_projection_mut<R>(&self, f: impl FnOnce(&L, &mut P) -> R) -> R {
        let mut inner = self.inner.lock().expect("poisoned");
        let Inner {
            log, projection, ..
        } = &mut *inner;
        f(log, projection)
    }

    /// Run lifecycle/repair work after synchronously sealing every group-commit write that was accepted
    /// before the lifecycle lock was acquired. This closes the gap between the coordinator's buffered
    /// commands and the authoritative log: reset/replay can only begin once all earlier waiters have a
    /// durable position and their live projection apply has completed.
    pub fn with_quiesced_log_and_projection_mut<R>(
        &self,
        f: impl FnOnce(&L, &mut P) -> EngineResult<R>,
    ) -> EngineResult<R> {
        let mut inner = self.inner.lock().expect("poisoned");
        if self.gc_active(&inner) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or(0);
            let shards: Vec<QueueKey> = inner
                .coords
                .iter()
                .filter(|(_, coordinator)| !coordinator.pending.is_empty())
                .map(|(shard, _)| shard.clone())
                .collect();
            for shard in shards {
                Self::gc_force_seal(&mut inner, &shard, now_ms)?;
            }
        }
        let Inner {
            log, projection, ..
        } = &mut *inner;
        f(log, projection)
    }

    /// Read, emit, and durably advance the change-record tail cursor for one shard.
    pub fn emit_change_record_tail<S: crate::port::ChangeRecordSink + ?Sized>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<fireweed_core::OwnerId>,
    ) -> EngineResult<usize> {
        let cursor = {
            let g = self.inner.lock().expect("composed backend poisoned");
            Self::require_known_shard(&g, shard)?;
            g.log.emission_cursor(shard)?
        };
        let page = self
            .inner
            .lock()
            .expect("composed backend poisoned")
            .log
            .read_from(shard, cursor.clone(), limit)?;
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
            let position: CommandPosition = position.clone();
            g.log.set_emission_cursor(shard, position)?;
        }
        Ok(records.len())
    }

    /// Reap terminal items that are now past the durable emission cursor for one shard.
    ///
    /// The emission gate stays fail-closed while the cursor is unavailable: emit-enabled queues only
    /// reap after the change-record emitter has durably advanced the cursor past the terminal record.
    fn reap_terminal_items_locked(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
    ) -> EngineResult<usize> {
        // TD-004 "Retention backpressure": retention advancement (terminal-item reaping here; the segment
        // expiry it enables) MUST stop while async-apply debt is over budget, lineage is unproven, or the
        // async SQLite worker is poisoned. Withhold reaping (advance nothing) when the projection reports
        // retention may not advance. No-op unless the projection is a hard-backpressured/poisoned
        // `objectlog/hybrid-async` store.
        if !inner.projection.retention_may_advance(shard) {
            return Ok(0);
        }
        let emission_cursor = if emit_change_records {
            inner.log.emission_cursor(shard)?
        } else {
            None
        };
        if emit_change_records && emission_cursor.is_none() {
            return Ok(0);
        }
        Ok(inner
            .projection
            .reap_terminal_items(
                shard,
                now,
                terminal_retention_ms,
                emit_change_records,
                emission_cursor.as_ref(),
            )?
            .len())
    }

    pub fn reap_terminal_items(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
    ) -> EngineResult<usize> {
        let mut g = self.inner.lock().expect("composed backend poisoned");
        Self::require_known_shard(&g, shard)?;
        Self::reap_terminal_items_locked(
            &mut g,
            shard,
            now,
            terminal_retention_ms,
            emit_change_records,
        )
    }

    /// Reclaim object-log SEGMENT OBJECTS whose commands are all past request-id retention AND already durably
    /// checkpointed (bead pqueue-b5cc2bc7). Runs under the composed unit-of-work lock right after the reap tick
    /// advances retention, so it never races the local writer or the manifest CAS.
    ///
    /// GATE: only when [`ProjectionStore::retention_may_advance`] is true (Clear / non-poisoned /
    /// lineage-proven). Under Hard async-apply debt or poison the reap already short-circuited and this returns
    /// without deleting anything OR advancing the floor — the durable floor is monotone and never advances past
    /// unproven debt. (Under Hard the checkpoint high-water is ALSO withheld, so this is belt-and-suspenders.)
    ///
    /// HORIZON: `trim_through = min(checkpoint_high_water_seq, max_trimmable_seq_before(cutoff))` with
    /// `cutoff = now - request_id_retention_ms - SKEW_MARGIN`. The `min` never trims past the durably-applied
    /// checkpoint (a below-floor command SQLite has not yet applied is never lost — the SQLite next_seq guard
    /// skips already-applied ones on replay), and the time term guarantees every trimmed segment holds ONLY
    /// expired request_ids (created_at <= committed_at_ms by causality), preserving AC-TXN-3.
    ///
    /// ORDER (crash-safe, MANDATORY): (b) advance the durable floor FIRST, THEN (c) delete the segment objects.
    /// A crash between them leaves floor=F with some below-F segments still present — recovery reads from F+1
    /// and skips them, no "missing segment" error. The reverse order would leave the floor pointing past a
    /// deleted segment. Returns the aggregate bounded-maintenance report.
    ///
    /// FINISH-INTERRUPTED-DELETION (bug 2a): a crash BETWEEN (b) and (c) — or a partial (c) — leaves segment
    /// objects at/below the durable floor undeleted. The in-memory `trim_completed_through` watermark is empty
    /// on process start, so the FIRST trim tick after a (re)open re-runs bounded expiry up to the
    /// durable floor to finish that deletion (idempotent), even when the newly-computed horizon does not
    /// advance. Once completed, the watermark suppresses re-scanning on idle ticks.
    ///
    /// EPOCH FENCE (bug 2b/3): the composed UoW lock is process-LOCAL and does not fence a peer owner. The floor
    /// advance is therefore an EPOCH-FENCED MANIFEST CAS inside `advance_retention_floor` — a superseded owner
    /// LOSES the CAS or is `EpochFenced` and cannot regress a newer owner's floor (which would strand recovery
    /// at a reclaimed segment). A fenced/raced advance is treated here as a benign skip (delete nothing, don't
    /// error the tick). BRANCH PINS (bug 2b): bounded expiry remains incomplete while a live pin blocks the
    /// target, so the completed-deletion watermark is written only after the pin has released.
    fn trim_reclaimable_segments_locked(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        request_id_retention_ms: u64,
        now: UtcTimestamp,
    ) -> EngineResult<MaintenanceSummary> {
        // (gate) Retention advancement must be permitted — mirrors the reap short-circuit at
        // `reap_terminal_items_locked`. No-op unless a hybrid-async projection reports Clear.
        if !inner.projection.retention_may_advance(shard) {
            return Ok(MaintenanceSummary::default());
        }
        let Some(expected_epoch) = inner.log.maintenance_owner_epoch(shard) else {
            return Ok(MaintenanceSummary::default());
        };
        if inner.log.current_epoch(shard)? != expected_epoch {
            return Ok(MaintenanceSummary::default());
        }
        let now_ms = ts_to_ms(now);
        let mut summary = MaintenanceSummary::default();

        // (bug 2a) FINISH any crash-interrupted deletion up to the DURABLE floor before considering new
        // reclamation. `trim_completed_through` is `None` (absent) on process start, so this runs once after
        // each (re)open per shard — INCLUDING when the durable floor is at sequence 0 (an absent watermark is
        // treated as "nothing completed yet", not 0, which a bare `0 < 0` comparison would wrongly skip). A
        // watermark held below a branch pin also re-triggers this until the pin releases.
        let durable_floor = inner.log.retention_floor(shard)?;
        if let Some(floor) = &durable_floor {
            let completed = inner.trim_completed_through.get(shard).copied();
            if completed.is_none_or(|c| c < floor.sequence) {
                let pass =
                    inner
                        .log
                        .expire_segments_through_bounded(shard, floor.sequence, now_ms)?;
                let complete = pass.deletion_pass_complete();
                summary.merge(pass);
                if !complete {
                    return Ok(summary);
                }
                Self::record_trim_watermark_locked(inner, shard, floor.sequence);
            }
        }

        // A non-empty claim-by-query replay lives through its lease expiry even when request-id retention is
        // shorter. Keep its originating command durable for the same interval; otherwise a restart after
        // segment reclamation would retain the leased projection row but lose the request outcome needed to
        // replay the claim.
        let in_memory_claim_replay_pinned = inner
            .claim_by_query_idempotency
            .get(shard)
            .is_some_and(|cache| {
                cache.has_unexpired_matching(now, |(item_ids, _)| !item_ids.is_empty())
            });

        // (a1) The durable checkpoint high-water — the highest seq the durable projection image has absorbed.
        // Under the Clear gate the hybrid-async monitor returns the REAL (un-withheld) value. `None` means
        // nothing is durably applied, so nothing is safe to trim.
        let checkpoint = inner.projection.recovery_high_water(shard)?;
        // (a2) The request-id-retention horizon: newest data segment whose commands are all past retention.
        let cutoff_ms = now_ms
            .saturating_sub(request_id_retention_ms as i64)
            .saturating_sub(RETENTION_TRIM_SKEW_MARGIN_MS);
        let time_expired_seq = inner.log.max_trimmable_seq_before(shard, cutoff_ms)?;
        let checkpoint_through = checkpoint.as_ref().map(|position| position.sequence);
        let proposed_through = checkpoint_through
            .zip(time_expired_seq)
            .map(|(checkpoint, time)| checkpoint.min(time))
            .unwrap_or(0);
        let complete_required = inner.projection.requires_complete_retention_frontier();
        let complete_proven = inner
            .projection
            .complete_retention_frontier_is_proven(shard);
        let authority = MaintenanceAuthoritySnapshot {
            queue: shard.clone(),
            current_epoch: expected_epoch,
            observed_at_ms: now_ms,
            retention_may_advance: inner.projection.retention_may_advance(shard),
            complete_frontier_required: complete_required,
            lineage_validated: !complete_required || complete_proven,
            committed_snapshot_through: checkpoint_through,
            recovery_window_through: time_expired_seq,
            manifest_tail: if complete_required {
                crate::FrontierRequirement::Unknown
            } else {
                crate::FrontierRequirement::NotRequired
            },
            request_ids: if complete_required {
                crate::FrontierRequirement::Unknown
            } else {
                crate::FrontierRequirement::NotRequired
            },
            item_keys: if complete_required {
                crate::FrontierRequirement::Unknown
            } else {
                crate::FrontierRequirement::NotRequired
            },
            async_projection_through: None,
            in_memory_claim_replay: if in_memory_claim_replay_pinned {
                crate::FrontierRequirement::RequiredFrom(0)
            } else {
                crate::FrontierRequirement::NotRequired
            },
            durable_floor: durable_floor.as_ref().map(|position| position.sequence),
            branch_pins: BTreeSet::new(),
        };
        let candidate = MaintenanceCandidate {
            queue: shard.clone(),
            stable_id: format!("segment-prefix-through-{proposed_through}"),
            class: MaintenanceObjectClass::SegmentPrefix,
            first_sequence: durable_floor
                .as_ref()
                .map_or(Some(0), |floor| floor.sequence.checked_add(1)),
            last_sequence: Some(proposed_through),
            manifest_index: None,
            bytes: None,
            created_at_ms: cutoff_ms,
            unreferenced_proven: true,
            loser_proven: false,
        };
        let decision = MaintenancePolicy::new(0)
            .plan(&authority, &[candidate], &MaintenanceFilter::default())
            .into_iter()
            .next()
            .expect("one retention candidate");
        if decision.disposition != MaintenanceDisposition::Delete {
            return Ok(summary);
        }
        let trim_through_seq = decision
            .candidate
            .last_sequence
            .expect("segment prefix carries a sequence");
        let checkpoint = checkpoint.expect("eligible retention requires a checkpoint");
        // The owner's currently-held epoch — re-read authoritatively inside `advance_retention_floor` against
        // the manifest tail, so a superseded owner is fenced (bug 3). Stamp the floor position with the
        // checkpoint epoch (<= held epoch), which keeps the recovery-start `max_position` compare well-defined
        // (`trim_through_seq <= checkpoint.sequence`, so the floor never exceeds the checkpoint position).
        let floor_pos =
            CommandPosition::new(shard.clone(), checkpoint.backend_epoch, trim_through_seq);
        // (b) Durable floor FIRST via the epoch-fenced manifest CAS. A fenced/raced advance means another owner
        // is authoritative here — skip cleanly (delete nothing) rather than deleting under a floor we did not
        // durably set.
        let newly_deleted =
            AdvanceThenDelete::<RetentionFloorClass, RetainedAddress>::publish_then_delete(
                inner,
                |inner| {
                    inner
                        .log
                        .advance_retention_floor(shard, floor_pos, expected_epoch)
                },
                |inner| {
                    inner
                        .log
                        .expire_segments_through_bounded(shard, trim_through_seq, now_ms)
                },
            );
        match newly_deleted {
            Ok(pass) => {
                let complete = pass.deletion_pass_complete();
                summary.merge(pass);
                if !complete {
                    return Ok(summary);
                }
            }
            Err(EngineError::EpochFenced) | Err(EngineError::Conflict) => return Ok(summary),
            Err(error) => return Err(error),
        }
        // (c) The typed boundary has now deleted the segment objects after the floor publication. Record the
        // completed watermark only after the bounded pass proved the whole target complete.
        Self::record_trim_watermark_locked(inner, shard, trim_through_seq);
        Ok(summary)
    }

    /// Record completion only after the bounded expiry pass returned without a cursor/stop/fence/failure.
    /// That pass pages the complete branch-pin registry before deleting and deliberately remains incomplete
    /// while any live pin blocks the target. Re-listing the full registry and manifest here duplicated that
    /// proof with unbounded provider I/O under the composed lock.
    fn record_trim_watermark_locked(inner: &mut Inner<L, P>, shard: &QueueKey, target: u64) {
        inner.trim_completed_through.insert(shard.clone(), target);
    }

    /// Capture only composition-owned authority while the queue-local permit and global mutex are held.
    /// Provider-backed log reads deliberately do not happen here.
    fn prepare_detached_retention_locked(
        inner: &Inner<L, P>,
        shard: &QueueKey,
        request_id_retention_ms: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<DetachedRetentionRequest>> {
        if !inner.projection.retention_may_advance(shard) {
            return Ok(None);
        }
        let Some(expected_epoch) = inner.log.maintenance_owner_epoch(shard) else {
            return Ok(None);
        };
        let complete_required = inner.projection.requires_complete_retention_frontier();
        let complete_proven = inner
            .projection
            .complete_retention_frontier_is_proven(shard);
        let in_memory_claim_replay_pinned = inner
            .claim_by_query_idempotency
            .get(shard)
            .is_some_and(|cache| {
                cache.has_unexpired_matching(now, |(item_ids, _)| !item_ids.is_empty())
            });
        let now_ms = ts_to_ms(now);
        Ok(Some(DetachedRetentionRequest {
            shard: shard.clone(),
            expected_epoch,
            now_ms,
            cutoff_ms: now_ms
                .saturating_sub(request_id_retention_ms as i64)
                .saturating_sub(RETENTION_TRIM_SKEW_MARGIN_MS),
            checkpoint: inner.projection.recovery_high_water(shard)?,
            // Complete-frontier stores intentionally remain withheld until every frontier axis is represented
            // in the authority snapshot. This preserves the previous policy's fail-closed Unknown fields.
            allow_floor_advance: !complete_required
                && complete_proven
                && !in_memory_claim_replay_pinned,
            completed_through: inner.trim_completed_through.get(shard).copied(),
        }))
    }

    /// Revalidate process-local authority after provider I/O before publishing its process-local progress.
    /// Durable deletion safety does not rely on this watermark: the detached handle already fenced the floor
    /// CAS and every destructive request. A raced owner/projection state simply leaves the watermark absent so
    /// a later tick re-scans idempotently.
    fn finalize_detached_retention_locked(
        inner: &mut Inner<L, P>,
        shard: &QueueKey,
        outcome: &DetachedRetentionOutcome,
    ) {
        if inner.log.maintenance_owner_epoch(shard) != Some(outcome.expected_epoch)
            || !inner.projection.retention_may_advance(shard)
        {
            inner.trim_completed_through.remove(shard);
            return;
        }
        match outcome.watermark {
            DetachedTrimWatermark::Unchanged => {}
            DetachedTrimWatermark::Clear => {
                inner.trim_completed_through.remove(shard);
            }
            DetachedTrimWatermark::Set(sequence) => {
                inner.trim_completed_through.insert(shard.clone(), sequence);
            }
        }
    }

    fn trim_reclaimable_segments_detached(
        &self,
        shard: &QueueKey,
        request_id_retention_ms: u64,
        now: UtcTimestamp,
    ) -> EngineResult<MaintenanceSummary> {
        {
            let inner = self.inner.lock().expect("composed backend poisoned");
            Self::require_known_shard(&inner, shard)?;
        }
        let Some(handle) = self.detached_maintenance.as_ref() else {
            let mut inner = self.inner.lock().expect("composed backend poisoned");
            return Self::trim_reclaimable_segments_locked(
                &mut inner,
                shard,
                request_id_retention_ms,
                now,
            );
        };
        let request = {
            let inner = self.inner.lock().expect("composed backend poisoned");
            Self::prepare_detached_retention_locked(&inner, shard, request_id_retention_ms, now)?
        };
        let Some(request) = request else {
            return Ok(MaintenanceSummary::default());
        };
        let outcome = handle.execute_retention(request)?;
        {
            let mut inner = self.inner.lock().expect("composed backend poisoned");
            Self::finalize_detached_retention_locked(&mut inner, shard, &outcome);
        }
        Ok(outcome.summary)
    }

    /// Public entry to [`Self::trim_reclaimable_segments_locked`] (the background sink loop drives this after
    /// its reap, mirroring the reap tick). Acquires the unit-of-work lock.
    pub fn trim_reclaimable_segments(
        &self,
        shard: &QueueKey,
        request_id_retention_ms: u64,
        now: UtcTimestamp,
    ) -> EngineResult<MaintenanceSummary> {
        self.trim_reclaimable_segments_detached(shard, request_id_retention_ms, now)
    }

    /// Async port-facing retention helper. Work begins only when polled and is queue-serialized.
    pub fn trim_reclaimable_segments_async(
        &self,
        shard: QueueKey,
        request_id_retention_ms: u64,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<MaintenanceSummary>> + Send {
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            self.trim_reclaimable_segments(&shard, request_id_retention_ms, now)
        })
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
        // The group-commit buffer is itself a durable-write admission boundary. Centralizing the gate here
        // covers every buffered command, even when a caller omitted an operation-specific preflight.
        inner.projection.admit_mutation(shard)?;
        let (serialized, permit) =
            Self::prepare_byte_admission(inner, shard, std::slice::from_ref(&env))?;
        let slot = Arc::new(SealSlot::new());
        let now_ms = ts_to_ms(now);
        let resolved_epoch = match expected_epoch {
            Some(e) => e,
            None => inner.log.current_epoch(shard)?,
        };
        let inner_queue_limit = inner.queue_byte_limit;
        let enqueued = {
            let Inner { log, coords, .. } = &mut *inner;
            let coord = coords.entry(shard.clone()).or_default();
            if !coord.pending.is_empty()
                && let (Some(limit), Some(new_permit)) = (inner_queue_limit, permit.as_ref())
            {
                let queue_bytes: usize = coord
                    .permits
                    .iter()
                    .flatten()
                    .map(OwnedBytePermit::bytes)
                    .sum();
                if queue_bytes.saturating_add(new_permit.bytes()) > limit {
                    return Err(EngineError::Backpressure {
                        resource: "queue buffered bytes",
                    });
                }
            }
            if coord.pending.is_empty() {
                coord.seal_epoch = resolved_epoch;
            }
            coord.pending.push(env);
            coord.permits.push(permit);
            coord.waiters.push(slot.clone());
            // Enqueue by reference (no per-command envelope clone on the hot path); the seal epoch is the
            // batch's, so co-buffered commands seal together under one epoch.
            if serialized.is_empty() {
                log.gc_enqueue(
                    shard,
                    std::slice::from_ref(coord.pending.last().expect("just pushed")),
                    coord.seal_epoch,
                    now_ms,
                )
            } else {
                log.gc_enqueue_serialized(
                    shard,
                    std::slice::from_ref(coord.pending.last().expect("just pushed")),
                    serialized,
                    coord.seal_epoch,
                    now_ms,
                )
            }
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
        let permits: Vec<Option<OwnedBytePermit>> = coord.permits.drain(..n).collect();
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
        drop(permits);
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
            coord.permits.clear();
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
        let supports_gates = inner.projection.supports_gates();
        for env in &envs {
            validate_gate_command(supports_gates, &env.command)?;
            validate_request_replay_metadata(env)?;
        }
        if envs.is_empty() {
            return Ok(());
        }
        inner.projection.admit_mutation(shard)?;
        let (serialized, permit) = Self::prepare_byte_admission(inner, shard, &envs)?;
        let now_ms = ts_to_ms(envs[0].created_at);
        let seal_epoch = match expected_epoch {
            Some(e) => e,
            None => inner.log.current_epoch(shard)?,
        };
        let mut positions = if serialized.is_empty() {
            inner.log.gc_enqueue(shard, &envs, seal_epoch, now_ms)?
        } else {
            inner
                .log
                .gc_enqueue_serialized(shard, &envs, serialized, seal_epoch, now_ms)?
        };
        if positions.is_empty() {
            positions = inner.log.gc_seal(shard, seal_epoch, now_ms)?;
        }
        if let Some(last) = positions.last() {
            inner.log.gc_advance_high_water(shard, last.clone())?;
        }
        let result = inner.projection.apply_live_owned(positions, envs);
        drop(permit);
        result
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
    fn gc_active(&self, _inner: &Inner<L, P>) -> bool {
        self.group_commit && self.supports_group_commit
    }

    /// Seal every latency-due queue's buffered batch + distribute it (ADR-012 P2 externalized flusher). The
    /// runtime-bearing crate (`fireweed-server`, which has tokio) drives this on an interval at
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
            Self::require_known_shard(&g, &shard)?;
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

    /// Deferred async entrypoint for the group-commit latency flusher.
    pub fn flush_tick_async(&self, now_ms: i64) -> impl Future<Output = EngineResult<()>> + Send {
        deferred(move || self.flush_tick(now_ms))
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

    /// Deferred async entrypoint for projection repair/checkpoint draining.
    pub fn flush_deferred_projection_async(&self) -> impl Future<Output = EngineResult<()>> + Send {
        deferred(move || self.flush_deferred_projection())
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
    /// Recovery-on-open whose storage work starts on first poll.
    pub fn recover_async(self) -> impl Future<Output = EngineResult<Self>> + Send {
        deferred(move || self.recover())
    }

    pub fn recover(self) -> EngineResult<Self> {
        self.run_recovery()?;
        Ok(self)
    }

    fn run_recovery(&self) -> EngineResult<()> {
        let mut max_cmd_seq: Option<u64> = None;
        // Page the projection catalog first and the log catalog second. A page can be empty after worker
        // partition filtering while still carrying a cursor over the underlying storage rows.
        for projection_catalog in [true, false] {
            let mut cursor = None;
            loop {
                let page = {
                    let g = self.inner.lock().expect("composed backend poisoned");
                    if projection_catalog {
                        g.projection.recover_definitions_page(
                            cursor.as_ref(),
                            DEFINITION_PAGE_LIMIT,
                            self.worker_partition,
                        )?
                    } else {
                        g.log.recover_definitions_page(
                            cursor.as_ref(),
                            DEFINITION_PAGE_LIMIT,
                            self.worker_partition,
                        )?
                    }
                };
                for def in page.definitions {
                    let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
                    // Repopulate the in-process control plane (idempotent for a compatible re-create).
                    self.control.create_queue(def.clone())?;
                    let mut g = self.inner.lock().expect("composed backend poisoned");
                    if !g.known_shards.insert(key.clone()) {
                        continue;
                    }
                    let Inner {
                        log,
                        projection,
                        idempotency,
                        claim_by_query_idempotency,
                        commit_idempotency,
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
                    // The durable retention floor (bead pqueue-b5cc2bc7): the highest command position whose segment
                    // OBJECTS have been trimmed, an EXCLUSIVE lower bound. `None` (a never-trimmed / pre-floor log)
                    // means genesis, so every fold below starts from the beginning — BYTE-IDENTICAL to a pre-floor
                    // log. When a trim HAS run, the below-floor segments are gone from the store, so both idempotency
                    // folds AND the projection replay must start at `floor + 1` (the trim guarantees every below-floor
                    // request_id is already past request_id_retention_ms, so none is dropped — see AC-TXN-3 proof).
                    let floor = log.retention_floor(&key)?;
                    // Rebuild the in-memory `request_id -> result` push-idempotency map from the durable log for
                    // EVERY composed-log backend, not only the eventual-apply ones. `push_with_request_id`
                    // consults/records only this in-memory map (see the `check`/`record` calls), which starts
                    // empty on reopen. Atomic composed-log backends (sqlite/postgres, DurabilityClass::Atomic)
                    // durably record the request_id + request_outcome on the log at commit time but previously
                    // did NOT rebuild the map on recovery, so a post-restart retry of an already-committed
                    // request_id re-executed instead of replaying its one committed result — a violation of the
                    // unknown-outcome contract (INV-14). The rebuild is a pure log fold and is correct for both
                    // durability classes (the relational, DB-authoritative family is a separate backend type, not
                    // a ComposedBackend, and is unaffected).
                    let recovered_max_cmd_seq = Self::rebuild_idempotency_from_log(
                        log,
                        RecoveryIdempotencyCaches {
                            push: idempotency,
                            claim: claim_by_query_idempotency,
                            commit: commit_idempotency,
                        },
                        &key,
                        def.request_id_retention_ms,
                        floor.clone(),
                        &self.counters,
                    )?;
                    // Symmetric rebuild for the OTHER request_id-bearing mutating op: `commit_transition` (the
                    // authoritative vectorized claimed-work commit). Its in-memory `commit_idempotency` cache is
                    // likewise empty on reopen; without this rebuild a post-restart retry of an already-committed
                    // request_id would NOT replay the one committed per-entry outcome — it would be lease-fenced
                    // (input already finalized) and reject (0-duplicate, but not an unknown-outcome cached replay,
                    // violating INV-14 for commit_transition). The rebuild is a pure durable-state reconstruction:
                    // the committed per-entry `EntryRecovery` is rebuilt from the durable commit envelopes on the
                    // log (Finalize/WriteSideRecords/AdvanceInstanceFence/Push), and the body fingerprint is the one
                    // stamped onto those envelopes at commit time. See `rebuild_idempotency_from_log`.
                    if let Some(sequence) = recovered_max_cmd_seq {
                        max_cmd_seq = Some(max_cmd_seq.map_or(sequence, |max| max.max(sequence)));
                    }
                    // Replay the durable log tail from the projection's recovery high-water (genesis when `None`),
                    // after the poison/backpressure gate above resolves whether that high-water is trustworthy.
                    let recorded_high_water = projection.recovery_high_water(&key)?;
                    // FAIL-CLOSED (bead pqueue-b5cc2bc7 bug 3): if this shard has a durable retention floor, the
                    // below-floor object-log segments are RECLAIMED, so the durable projection image MUST already
                    // cover the floor (`recovery_high_water >= floor`). It always does for a consistent store (the
                    // floor was advanced only while `checkpoint >= floor`, and the checkpoint is monotone). A
                    // projection BEHIND the floor — a restored, rolled-back, or FOREIGN SQLite image over a trimmed
                    // log — would make the R1 replay-start flooring omit the commands between the image and the floor
                    // (absent from BOTH the reclaimed log AND the behind image): a SILENT data loss. Refuse to serve.
                    // (At reopen the async-apply monitor is Clear — it is memoryless across restart — so
                    // `recovery_high_water` here is the REAL durable high-water, not a withheld one; a poisoned
                    // projection already failed in `resolve_recovery_start` below.)
                    if let Some(fl) = &floor {
                        let covers_floor = recorded_high_water
                            .as_ref()
                            .is_some_and(|hw| hw.sequence >= fl.sequence);
                        if !covers_floor {
                            let hw_seq = recorded_high_water.as_ref().map(|hw| hw.sequence);
                            return Err(EngineError::Storage(format!(
                                "read below retention floor: projection high-water {:?} <= reclaimed floor {} \
                         (recovery refused: the projection image is behind the durable floor; a restored, \
                         rolled-back, or foreign projection image over a trimmed log is an unrecoverable \
                         inconsistency)",
                                hw_seq, fl.sequence,
                            )));
                        }
                    }
                    let resolved_start = match resolve_recovery_start(
                        recovery_poison.as_deref(),
                        hard_backpressure,
                        recorded_high_water,
                    )? {
                        RecoveryStart::FromHighWater(pos) => pos,
                        RecoveryStart::FromGenesis => None,
                    };
                    // R1 FIX (bead pqueue-b5cc2bc7): floor the replay start at the durable retention floor. Under Hard
                    // backpressure `resolve_recovery_start` returns `FromGenesis` (start = None) which, on a trimmed
                    // log, would read a DELETED below-floor segment and fail "missing segment". Flooring is safe: the
                    // floor is <= checkpoint_high_water at trim time, and the durable SQLite next_seq guard skips any
                    // below-floor command it has already applied when the tail replays over its durable image. The
                    // healthy path is unchanged — floor <= checkpoint, so the max is the checkpoint high-water.
                    let mut from = max_position(resolved_start, floor.clone());
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
                match page.next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
        }
        if let Some(m) = max_cmd_seq {
            let mut g = self.inner.lock().expect("composed backend poisoned");
            g.cmd_seq = g.cmd_seq.max(m + 1);
        }
        Ok(())
    }

    fn rebuild_idempotency_from_log(
        log: &L,
        caches: RecoveryIdempotencyCaches<'_>,
        shard: &QueueKey,
        retention_ms: u64,
        floor: Option<CommandPosition>,
        counters: &QueueCounters,
    ) -> EngineResult<Option<u64>> {
        let mut commit_accums = HashMap::new();
        let mut max_cmd_seq: Option<u64> = None;
        let mut from = floor;
        loop {
            let page = log.read_from(shard, from.clone(), RECOVERY_READ_PAGE_LIMIT)?;
            Self::fold_push_idempotency(&mut *caches.push, shard, retention_ms, &page.entries)?;
            Self::fold_claim_by_query_idempotency(
                &mut *caches.claim,
                shard,
                retention_ms,
                &page.entries,
            )?;
            Self::fold_commit_idempotency(&mut commit_accums, &page.entries)?;
            for (_, envelope) in &page.entries {
                for item_id in &envelope.item_ids {
                    counters.observe(shard, *item_id);
                }
                if let Some(sequence) = envelope
                    .command_id
                    .0
                    .rsplit('-')
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                {
                    max_cmd_seq = Some(max_cmd_seq.map_or(sequence, |max| max.max(sequence)));
                }
            }
            match page.next {
                Some(next) => from = Some(next),
                None => break,
            }
        }
        Self::finish_commit_idempotency(caches.commit, shard, retention_ms, commit_accums);
        Ok(max_cmd_seq)
    }

    fn fold_push_idempotency(
        idempotency: &mut HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
        shard: &QueueKey,
        retention_ms: u64,
        entries: &[(CommandPosition, CommandEnvelope)],
    ) -> EngineResult<()> {
        for (_, env) in entries {
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
                    // A `Push` command never carries a `CommitTransition` outcome; fall back to the
                    // envelope's minted ids (same as the `None` legacy-push path).
                    Some(RequestOutcome::ClaimByQuery { .. })
                    | Some(RequestOutcome::BatchUpdate { .. })
                    | Some(RequestOutcome::CommitTransition { .. })
                    | None => env.item_ids.clone(),
                },
                expires_at,
            );
        }
        Ok(())
    }

    fn fold_claim_by_query_idempotency(
        idempotency: &mut HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken)>>,
        shard: &QueueKey,
        retention_ms: u64,
        entries: &[(CommandPosition, CommandEnvelope)],
    ) -> EngineResult<()> {
        for (_, env) in entries {
            if let QueueCommand::RenewLease(renew) = &env.command {
                let renewed: HashSet<ItemId> = renew.item_ids.iter().copied().collect();
                idempotency
                    .entry(shard.clone())
                    .or_default()
                    .extend_expiry_matching(renew.lease_expires_at, |(item_ids, _)| {
                        !item_ids.is_empty()
                            && item_ids.iter().all(|item_id| renewed.contains(item_id))
                    });
                continue;
            }
            let (
                Some(request_id),
                Some(fingerprint),
                Some(RequestOutcome::ClaimByQuery {
                    item_ids,
                    lease_token,
                    ..
                }),
            ) = (
                &env.request_id,
                env.request_fingerprint,
                &env.request_outcome,
            )
            else {
                continue;
            };
            let expires_at = match (&env.command, item_ids.is_empty()) {
                (QueueCommand::Claim(claim), false) => {
                    request_expires_at(env.created_at, retention_ms).max(claim.lease_expires_at)
                }
                _ => request_expires_at(env.created_at, retention_ms),
            };
            idempotency.entry(shard.clone()).or_default().record(
                request_id.clone(),
                BodyHash(fingerprint),
                (item_ids.clone(), lease_token.clone()),
                expires_at,
            );
        }
        Ok(())
    }

    /// Recovery twin of `commit_transition`'s in-memory `commit_idempotency` record (mirrors
    /// [`Self::rebuild_idempotency_from_log`] fold for the OTHER request_id-bearing op). Rebuilds the
    /// `request_id -> (fingerprint, Vec<EntryRecovery>)` cache from the durable log so a post-restart retry of
    /// an already-committed `commit_transition` `request_id` replays the one committed per-entry outcome
    /// (INV-14 unknown-outcome replay) instead of re-executing / being lease-fenced.
    ///
    /// A `commit_transition` appends, per COMMITTED entry, an ordered run of envelopes all carrying the
    /// caller's `request_id` and the whole-body `request_fingerprint` (stamped at commit time) with NO
    /// `request_outcome`: `[WriteSideRecords?] [AdvanceInstanceFence?] [Push(lifecycle)?] Finalize`. The
    /// terminating `Finalize` (always emitted for a committed entry) delimits the entry. This fold groups the
    /// log by `request_id`, splits each group into entries at its `Finalize` boundaries, and reconstructs each
    /// committed [`EntryRecovery`] purely from durable state:
    /// `consumed_input_id` from the `Finalize` outcome, `side_record_keys` from `WriteSideRecords`, `instance`
    /// from `AdvanceInstanceFence`, `lifecycle_item_ids` from the lifecycle `Push`'s server-minted ids.
    ///
    /// `push_with_request_id` envelopes carry `request_outcome = Some(RequestOutcome::Push)` (handled by the
    /// push rebuild) and are skipped; a `commit_transition`'s own lifecycle `Push` carries
    /// `request_outcome = None` and so stays in the fold. A `request_id` with no stamped `request_fingerprint`
    /// (logs written before commit envelopes carried one) is skipped — its cross-restart replay stays
    /// unavailable, exactly as before, rather than being reconstructed without a conflict-detection fingerprint.
    ///
    /// REJECTION-bearing commits — MIXED committed+rejected AND ALL-REJECTED (bead pqueue-db60657d): a
    /// REJECTED entry mutates nothing and appends nothing durable of its own, so the piecemeal
    /// `Finalize`-delimited fold alone reconstructs only the COMMITTED entries (a SHORTER vec for a mixed
    /// commit; NOTHING for an all-rejected one). To replay such a commit faithfully, `commit_transition` stamps
    /// the WHOLE per-entry vec (committed AND rejected, each rejection's structured error projected via
    /// [`CommitRejection`]) onto a terminal marker envelope carrying [`RequestOutcome::CommitTransition`],
    /// appended in the SAME atomic batch as the committed entries. This fold treats that marker as
    /// AUTHORITATIVE (`durable_full`), superseding the piecemeal reconstruction, so the rebuilt record equals
    /// the live one and the `recovery.len() == entries.len()` replay guard passes — the retry replays
    /// byte-identically (including a time-dependent rejection that bare re-execution would resolve differently).
    ///
    /// BACK-COMPAT: a log written before the marker existed simply has no `CommitTransition` envelope, so
    /// `durable_full` stays `None` and the fold falls back to the committed-only piecemeal `entries`. An
    /// all-committed commit reconstructs exactly from its `Finalize` runs (no marker is written for it); a
    /// mixed commit in a pre-change log stays short and safely re-executes under the length guard, exactly as
    /// before — old logs are never corrupted or rejected.
    fn fold_commit_idempotency(
        accums: &mut HashMap<RequestId, CommitRecoveryAccum>,
        entries: &[(CommandPosition, CommandEnvelope)],
    ) -> EngineResult<()> {
        for (_, env) in entries {
            let Some(request_id) = &env.request_id else {
                continue;
            };
            let Some(fingerprint) = env.request_fingerprint else {
                continue;
            };
            // A `commit_transition`'s terminal marker carries the FULL per-entry outcome for a mixed commit.
            // Capture it as authoritative; it delimits nothing (no `Finalize`) so it stays out of the
            // piecemeal fold below.
            if let Some(RequestOutcome::CommitTransition { entries }) = &env.request_outcome {
                let accum =
                    accums
                        .entry(request_id.clone())
                        .or_insert_with(|| CommitRecoveryAccum {
                            fingerprint,
                            created_at: env.created_at,
                            pending_side_keys: Vec::new(),
                            pending_instance: None,
                            pending_lifecycle_ids: Vec::new(),
                            entries: Vec::new(),
                            durable_full: None,
                        });
                accum.durable_full = Some(
                    entries
                        .iter()
                        .cloned()
                        .map(recovery_from_outcome_entry)
                        .collect(),
                );
                continue;
            }
            // `push_with_request_id` envelopes carry `request_outcome = Some(RequestOutcome::Push)`; the push
            // rebuild owns them. Only `commit_transition` envelopes (request_outcome == None) belong to the
            // piecemeal fold.
            if env.request_outcome.is_some() {
                continue;
            }
            let accum = accums
                .entry(request_id.clone())
                .or_insert_with(|| CommitRecoveryAccum {
                    fingerprint,
                    created_at: env.created_at,
                    pending_side_keys: Vec::new(),
                    pending_instance: None,
                    pending_lifecycle_ids: Vec::new(),
                    entries: Vec::new(),
                    durable_full: None,
                });
            match &env.command {
                QueueCommand::WriteSideRecords(cmd) => {
                    accum.pending_side_keys = cmd.records.iter().map(|r| r.key.clone()).collect();
                }
                QueueCommand::AdvanceInstanceFence(cmd) => {
                    accum.pending_instance = Some((cmd.instance_key.clone(), cmd.next));
                }
                QueueCommand::Push(_) => {
                    accum.pending_lifecycle_ids = env.item_ids.clone();
                }
                QueueCommand::Finalize(cmd) => {
                    let Some(consumed_input_id) = cmd
                        .outcomes
                        .first()
                        .map(|o| o.item_id)
                        .or_else(|| env.item_ids.first().copied())
                    else {
                        continue;
                    };
                    let additional_consumed_input_ids = cmd
                        .outcomes
                        .iter()
                        .skip(1)
                        .map(|outcome| outcome.item_id)
                        .collect();
                    let side_record_keys = std::mem::take(&mut accum.pending_side_keys);
                    let instance = accum.pending_instance.take();
                    let lifecycle_item_ids = std::mem::take(&mut accum.pending_lifecycle_ids);
                    accum.entries.push(EntryRecovery {
                        consumed_input_id,
                        additional_consumed_input_ids,
                        instance,
                        side_record_keys,
                        lifecycle_item_ids,
                        status: CommitEntryStatus::Committed,
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finish_commit_idempotency(
        commit_idempotency: &mut HashMap<QueueKey, QueueIdempotencyCache<Vec<EntryRecovery>>>,
        shard: &QueueKey,
        retention_ms: u64,
        accums: HashMap<RequestId, CommitRecoveryAccum>,
    ) {
        for (request_id, accum) in accums {
            // A durable `CommitTransition` marker (mixed commit) is authoritative — it holds the whole vec
            // including the rejected entries; otherwise fall back to the committed-only piecemeal `entries`.
            let entries = accum.durable_full.unwrap_or(accum.entries);
            if entries.is_empty() {
                continue;
            }
            let expires_at = request_expires_at(accum.created_at, retention_ms);
            commit_idempotency.entry(shard.clone()).or_default().record(
                request_id,
                BodyHash(accum.fingerprint),
                entries,
                expires_at,
            );
        }
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
        let supports_gates = inner.projection.supports_gates();
        for env in &envs {
            validate_gate_command(supports_gates, &env.command)?;
        }
        if envs.is_empty() {
            return Ok(());
        }
        // Every non-group-commit mutation funnels through this append boundary. Admission belongs here so
        // lifecycle-offline, async poison, and hard-debt state reject finalize/renew/reassign/purge/upsert
        // and future mutation kinds just as reliably as push/claim.
        inner.projection.admit_mutation(shard)?;
        let (serialized, permit) = Self::prepare_byte_admission(inner, shard, &envs)?;
        let epoch = inner.log.current_epoch(shard)?;
        // ADR-009 / TD-003: an owner that supplies its cached acquire-time epoch (`Some`) is fenced here if
        // superseded; `None` is the degenerate sole-owner path (stamp current, never fence).
        if expected_epoch.is_some_and(|e| e != epoch) {
            return Err(EngineError::EpochFenced);
        }
        let positions = if serialized.is_empty() {
            inner.log.append(shard, &envs, epoch)?
        } else {
            inner
                .log
                .append_serialized(shard, &envs, serialized, epoch)?
        };
        let result = inner.projection.apply_live(&positions, &envs);
        drop(permit);
        result
    }

    fn prepare_byte_admission(
        inner: &Inner<L, P>,
        shard: &QueueKey,
        envs: &[CommandEnvelope],
    ) -> EngineResult<(Vec<Vec<u8>>, Option<OwnedBytePermit>)> {
        let Some(budget) = inner.byte_budget.as_ref() else {
            return Ok((Vec::new(), None));
        };
        let serialized = envs
            .iter()
            .map(|env| {
                serde_json::to_vec(env).map_err(|error| {
                    EngineError::Storage(format!("command serialization failed: {error}"))
                })
            })
            .collect::<EngineResult<Vec<_>>>()?;
        // Current segment framing: 21-byte header + record-count, then length+record per command. Charge
        // retained records and the simultaneously resident sealed frame, with checked arithmetic.
        let charged = retained_records_plus_frame_bytes(serialized.iter().map(Vec::len), 25, 4)
            .ok_or(EngineError::RequestTooLarge {
                requested: usize::MAX,
                limit: budget.config().global_limit(),
            })?;
        let permit = budget
            .try_acquire(shard.tenant_id.clone(), charged)
            .map_err(map_composed_byte_admission_error)?;
        Ok((serialized, Some(permit)))
    }

    /// The NON-item claim path (whole-group / same-group-key / whole-cohort, BQ-14b/c). The projection axis
    /// selects the candidates (and, for whole-cohort, the cohort id) under the composition's unit-of-work
    /// lock; the composition then commits the lease as a plain `Claim` (group / same-group) or a
    /// `CohortClaim` (whole-cohort — its apply arm also flips `pqueue_cohorts` to leased) through the atomic
    /// write seam, and renders the reply. A projection without a group/cohort read model refuses the
    /// selection with `Unavailable`, so the log-replay family rejects non-item units unchanged.
    fn claim_rich(&self, req: &ClaimRequest, unit: ClaimUnit) -> EngineResult<Claimed> {
        let mut g = self.inner.lock().expect("poisoned");
        // Due-ness at the caller-resolved eligibility epoch (see `ClaimRequest::eligibility_at`); the lease
        // and the committed command below are still stamped with the operational `req.now`.
        let selection = g.projection.select_rich_claim(
            &req.shard,
            unit,
            &req.compatibility,
            req.eligibility_at(),
            req.max_items,
        )?;
        if selection.item_ids.is_empty() {
            return Ok(Claimed::default());
        }
        let candidates = selection.item_ids;
        let command = if let Some(cohort_id) = selection.cohort_id.clone() {
            QueueCommand::CohortClaim(CohortClaimCommand {
                cohort_id,
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
            })
        } else {
            QueueCommand::Claim(ClaimCommand {
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
                worker_id: Some(req.worker_id.clone()),
            })
        };
        let env = Self::make_envelope(&mut g, self.node_id, command, candidates.clone(), req.now);
        Self::commit_locked(&mut g, &req.shard, env, req.expected_epoch)?;
        let items = g.projection.render_claimed(&req.shard, &candidates)?;
        debug_assert_eq!(
            items.len(),
            candidates.len(),
            "every rich-claim candidate must render"
        );
        let mut claimed = Claimed {
            items,
            ..Default::default()
        };
        if matches!(unit, ClaimUnit::WholeCohort) {
            // API-001 whole-cohort response shape: the shared lease token + cohort id ride at the top level;
            // the per-item rows omit their lease token (the cohort holds the single lease).
            claimed.cohort_lease_token = Some(req.lease_token.clone());
            claimed.cohort_id = selection.cohort_id;
            for item in &mut claimed.items {
                item.lease_token = None;
            }
        }
        Ok(claimed)
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
pub(crate) fn push_body_hash(items: &[PushSpec]) -> EngineResult<BodyHash> {
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

pub fn push_specs_fingerprint_sha256(items: &[PushSpec]) -> EngineResult<[u8; 32]> {
    #[derive(serde::Serialize)]
    struct Canonical<'a> {
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
        .map(|item| Canonical {
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
    push_body_sha256_canonical(&canonical)
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

pub fn push_items_fingerprint_sha256(items: &[PushItem]) -> EngineResult<[u8; 32]> {
    #[derive(serde::Serialize)]
    struct Canonical<'a> {
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
        .map(|item| Canonical {
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
    push_body_sha256_canonical(&canonical)
}

fn push_body_sha256_canonical<T: serde::Serialize>(items: &[T]) -> EngineResult<[u8; 32]> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(bytes).into())
}

fn push_body_hash_canonical<T: serde::Serialize>(items: &[T]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

fn claim_by_query_body_hash(request: &ClaimByQueryRequest) -> EngineResult<BodyHash> {
    use sha2::{Digest, Sha256};

    let mut canonical = request.clone();
    canonical.request_id = None;
    let bytes = serde_json::to_vec(&canonical).map_err(|e| EngineError::Storage(e.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(BodyHash(u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    )))
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

/// Project one [`EntryRecovery`] to its durable serializable form for [`RequestOutcome::CommitTransition`].
/// The inverse is [`recovery_from_outcome_entry`]; together they let a mixed commit's whole per-entry vec
/// (committed AND rejected, with the rejection's structured error) round-trip through the durable log.
fn outcome_entry_from_recovery(r: &EntryRecovery) -> CommitOutcomeEntry {
    CommitOutcomeEntry {
        consumed_input_id: r.consumed_input_id,
        additional_consumed_input_ids: r.additional_consumed_input_ids.clone(),
        instance: r.instance.clone(),
        side_record_keys: r.side_record_keys.clone(),
        lifecycle_item_ids: r.lifecycle_item_ids.clone(),
        rejection: match &r.status {
            CommitEntryStatus::Committed => None,
            CommitEntryStatus::Rejected(e) => Some(CommitRejection::from_error(e)),
        },
    }
}

/// Reconstruct an [`EntryRecovery`] from its durable serializable form (inverse of
/// [`outcome_entry_from_recovery`]).
fn recovery_from_outcome_entry(e: CommitOutcomeEntry) -> EntryRecovery {
    let status = match e.rejection {
        None => CommitEntryStatus::Committed,
        Some(rej) => CommitEntryStatus::Rejected(rej.into_error()),
    };
    EntryRecovery {
        consumed_input_id: e.consumed_input_id,
        additional_consumed_input_ids: e.additional_consumed_input_ids,
        instance: e.instance,
        side_record_keys: e.side_record_keys,
        lifecycle_item_ids: e.lifecycle_item_ids,
        status,
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> Backend for ComposedBackend<L, P, C> {
    fn durability_class(&self) -> DurabilityClass {
        self.durability
    }

    /// Whether the composition stores gate membership + enforces `SetGates` at claim selection — it inherits
    /// this from its projection axis (the relational projection has the gate tables; the log-replay family
    /// does not), so a gate-bearing push / `SetGates` is admitted iff the projection can materialize it.
    fn supports_gates(&self) -> bool {
        self.supports_gates
    }

    /// The authoritative-commit capabilities (Snorri StateStore boundary, epic pqueue-2201fd37). The
    /// composition advertises the FULL vectorized-commit guarantees iff BOTH axes support it: the projection
    /// materializes the commit-class read model (`supports_commit_transition`) AND the log gives an atomic
    /// append+apply boundary. Otherwise it advertises the all-false default so a consumer (Snorri) rejects it
    /// before activation. This reaches parity with the monolithic `MemoryBackend` for the composed memory
    /// backend (`MemoryLog × InMemoryProjection`).
    fn commit_capabilities(&self) -> CommitCapabilities {
        if self.supports_commit_transition && self.is_atomic() {
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

    fn commit_raw(
        &self,
        request: crate::RawCommitRequest,
    ) -> impl Future<Output = EngineResult<crate::RawCommitOutcome>> + Send {
        deferred(move || {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            if fault == crate::RawCommitFault::BeforeAppend {
                return Err(EngineError::Invalid("fault-injection: kill before append"));
            }
            let fault_hook = self
                .fault_hook
                .lock()
                .expect("compose fault hook poisoned")
                .clone();
            let mut g = self.inner.lock().expect("composed backend poisoned");
            if !g.known_shards.contains(&shard) {
                return Err(EngineError::NotFound);
            }
            g.projection.admit_mutation(&shard)?;
            let Inner {
                log, projection, ..
            } = &mut *g;
            let supports_gates = projection.supports_gates();
            for env in &commands {
                validate_gate_command(supports_gates, &env.command)?;
            }
            let positions = log.append(&shard, &commands, expected_epoch)?;
            if fault == crate::RawCommitFault::AfterAppendBeforeApply {
                return Ok(crate::RawCommitOutcome::appended(positions));
            }
            if let Some(hook) = &fault_hook {
                hook.fault_point(ComposeFaultPoint::DuringProjectionApply)?;
            }
            projection.apply_live(&positions, &commands)?;
            if let Some(hook) = &fault_hook {
                hook.fault_point(ComposeFaultPoint::AfterApplyBeforeResponse)?;
            }
            Ok(crate::RawCommitOutcome::applied(positions))
        })
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
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        queue_serialized(&self.mutation_gate, key.clone(), move || {
            let mut outcome = self.control.create_queue(definition)?;
            let mut g = self.inner.lock().expect("poisoned");
            let Inner {
                log,
                projection,
                known_shards,
                cmd_seq,
                ..
            } = &mut *g;
            log.ensure_shard(&key)?;
            let newly_known = !known_shards.contains(&key);
            if outcome.created {
                // Record the definition in the log's durable catalog so a reopened composition can recover
                // this queue without a re-`create_queue` (no-op for in-process / unified-relational logs).
                // A durable create-or-read catalog may replace the handle-local result with the decoded
                // cross-handle winner. Cache that winner before reporting either success or exact-definition
                // conflict so a losing handle never keeps serving its submitted loser definition.
                if let Some(durable) = log.create_or_read_definition(&outcome.definition)? {
                    let matches_submitted = durable.definition == outcome.definition;
                    self.control
                        .cache_authoritative_definition(durable.definition.clone())?;
                    outcome = durable;
                    if !matches_submitted {
                        return Err(EngineError::QueueDefinitionConflict);
                    }
                }
            }
            if outcome.created {
                projection.ensure_shard(&outcome.definition)?;
            } else if newly_known {
                // This handle was opened before another supported handle created the durable queue. The
                // object-log family is sole-owner for data-plane mutation, so handoff is ordered: commands
                // committed before this create must be replayed, while concurrent post-create mutation by
                // the old owner remains outside the contract. Start at a durable projection's checkpoint
                // (genesis for InMemoryProjection), while keeping this handle's serving image unpublished.
                let recovered_counter_high_water = projection.recovery_counter_high_water(&key)?;
                let recorded_high_water = projection.recovery_high_water(&key)?;
                let floor = log.retention_floor(&key)?;
                if let Some(floor) = &floor
                    && !recorded_high_water
                        .as_ref()
                        .is_some_and(|position| position.sequence >= floor.sequence)
                {
                    return Err(EngineError::Storage(format!(
                        "create loser projection is behind reclaimed retention floor {}",
                        floor.sequence
                    )));
                }
                let mut from = max_position(recorded_high_water, floor);
                let mut positions = Vec::new();
                let mut commands = Vec::new();
                let mut observed_item_ids = Vec::new();
                let mut next_cmd_seq = *cmd_seq;
                loop {
                    let page = log.read_from(&key, from.clone(), RECOVERY_READ_PAGE_LIMIT)?;
                    if !page.entries.is_empty() {
                        for (position, envelope) in page.entries {
                            observed_item_ids.extend(envelope.item_ids.iter().copied());
                            if let Some(sequence) = envelope
                                .command_id
                                .0
                                .rsplit('-')
                                .next()
                                .and_then(|value| value.parse::<u64>().ok())
                            {
                                next_cmd_seq = next_cmd_seq.max(sequence.saturating_add(1));
                            }
                            positions.push(position);
                            commands.push(envelope);
                        }
                    }
                    match page.next {
                        Some(next) => from = Some(next),
                        None => break,
                    }
                }
                // No externally visible state changes until every bounded page has been read successfully.
                // The in-memory object-log projection overrides this seam by building a scratch ProjectionData
                // and swapping it into place, so even a command-application error cannot leak a partial replay.
                projection.install_recovery_shard(&outcome.definition, &positions, &commands)?;
                if let Some(item_id) = recovered_counter_high_water {
                    self.counters.observe(&key, item_id);
                }
                for item_id in observed_item_ids {
                    self.counters.observe(&key, item_id);
                }
                *cmd_seq = next_cmd_seq;
            }
            // Publish the handle-local shard only after durable catalog persistence or loser replay
            // succeeds. A failed hydration remains retryable instead of leaving an empty known shard.
            known_shards.insert(key.clone());
            Ok(outcome)
        })
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        deferred(move || self.control.queue_definition(key))
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        deferred(move || self.control.list_queues(tenant))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .log
                .current_epoch(shard)
        })
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        deferred(move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
            g.log.acquire_epoch(shard)
        })
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
        Box::pin(async move {
            let permit = self
                .mutation_gate
                .acquire(shard.clone())
                .await
                .map_err(|_| EngineError::Unavailable)?;
            let pending = (|| {
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
                let mut g = self.inner.lock().expect("poisoned");
                Self::require_known_shard(&g, shard)?;
                let epoch = expected_epoch.unwrap_or(0);
                let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
                let (push_items, ids) =
                    build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
                g.projection.admit_mutation(shard)?;
                g.projection.index_validate_push(shard, &push_items)?;
                if g.projection.pause_blocks_intake(shard)? {
                    return Err(EngineError::Paused { drain_intake: true });
                }
                let env = Self::make_envelope(
                    &mut g,
                    self.node_id,
                    QueueCommand::Push(PushCommand { items: push_items }),
                    ids.clone(),
                    now,
                );
                if self.gc_active(&g) {
                    let slot = Self::gc_buffer(&mut g, shard, env, expected_epoch, now)?;
                    Ok::<_, EngineError>((Some(slot), ids))
                } else {
                    Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                    Ok((None, ids))
                }
            })()?;
            let (slot, ids) = pending;
            // Group-commit response waiting is not part of queue-local planning/append admission. Releasing
            // here permits later same-queue operations to join or force-seal the batch.
            drop(permit);
            if let Some(slot) = slot {
                SealWait::new(slot).await?;
            }
            Ok(ids)
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
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
            Self::require_known_shard(&g, shard)?;
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
            // TD-004 hard-debt admission gate — placed AFTER the idempotency Replay/Conflict resolution so it
            // gates only the PROCEED path (genuinely new work that adds async-apply debt). An idempotent
            // same-body retry of an already-committed `request_id` replays its committed ids above and adds
            // ZERO new debt, so it MUST NOT be rejected under Hard backpressure. No-op unless the projection
            // is a hard-backpressured `objectlog/hybrid-async` store.
            g.projection.admit_mutation(shard)?;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            g.projection.index_validate_push(shard, &push_items)?;
            if g.projection.pause_blocks_intake(shard)? {
                return Err(EngineError::Paused { drain_intake: true });
            }
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
        })
    }
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::RequestIdReplayProbe
    for ComposedBackend<L, P, C>
{
    /// Build the exact durable envelope [`Self::push_with_request_id`] would append — same
    /// `request_id` + `push_body_hash` fingerprint + [`RequestOutcome::Push`] + server-minted ids (reserving
    /// the counter/command-id identically) — WITHOUT committing it or recording the in-memory idempotency
    /// entry. Mirrors the non-group-commit body of `push_with_request_id` (validate gate/entity, reserve,
    /// `build_push_items`, `index_validate_push`) up to (but not including) the commit + record. The caller
    /// drives the returned envelope through [`crate::Backend::commit_raw`] with a mid-pipeline fault so the
    /// `AfterAppendBeforeApply` cut point carries a real `request_id` (recovery rebuilds the push-idempotency
    /// map from this durable envelope on reopen — see `rebuild_idempotency_from_log`).
    fn build_request_id_push_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, Vec<ItemId>)> {
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
        let mut g = self.inner.lock().expect("poisoned");
        Self::require_known_shard(&g, shard)?;
        let epoch = expected_epoch.unwrap_or(0);
        let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
        let (push_items, ids) =
            build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
        g.projection.index_validate_push(shard, &push_items)?;
        let command_id = Self::next_command_id(&mut g, self.node_id);
        let env = CommandEnvelope {
            command_id,
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: Some(RequestOutcome::Push {
                item_ids: ids.clone(),
            }),
            item_ids: ids.clone(),
            command: QueueCommand::Push(PushCommand { items: push_items }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        Ok((env, ids))
    }

    /// Build the exact durable FINALIZE envelope a SINGLE-entry [`Self::commit_transition`] would append —
    /// same `request_id` + whole-body `commit_body_hash` fingerprint (stamped in the SAME envelope field the
    /// real commit path now stamps) + the `Finalize` command over the consumed input — WITHOUT committing it
    /// or recording the in-memory `commit_idempotency` entry. Validates the `claim_ref` exactly like the real
    /// path (a `commit_validate` rejection here matches it). The caller drives the returned envelope through
    /// [`crate::Backend::commit_raw`] with a mid-pipeline fault so the `AfterAppendBeforeApply` cut point is
    /// `request_id`-bearing for `commit_transition`; recovery rebuilds the commit-idempotency cache from this
    /// durable envelope on reopen (see `rebuild_idempotency_from_log`).
    fn build_request_id_commit_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        claim_ref: ClaimRef,
        finalize: FinalizeKind,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, BodyHash)> {
        if !self.is_atomic() {
            return Err(EngineError::Unavailable);
        }
        // Fingerprint over the EXACT single-entry commit body `commit_transition(same body)` will hash, so a
        // post-reopen retry computes the identical fingerprint → Replay (not Conflict).
        let entry = crate::port::CommitTransitionEntry {
            claim_ref: claim_ref.clone(),
            additional_claim_refs: Vec::new(),
            finalize,
            side_records: Vec::new(),
            lifecycle_items: Vec::new(),
            instance_fence: None,
        };
        let fingerprint = commit_body_hash(std::slice::from_ref(&entry))?;
        let item_id = claim_ref.item_id;
        let mut g = self.inner.lock().expect("poisoned");
        Self::require_known_shard(&g, shard)?;
        if !g.projection.supports_commit_transition() {
            return Err(EngineError::Unavailable);
        }
        let _ = expected_epoch;
        g.projection
            .commit_validate(shard, std::slice::from_ref(&claim_ref), now)?;
        let command_id = Self::next_command_id(&mut g, self.node_id);
        let env = CommandEnvelope {
            command_id,
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(item_id, finalize)],
            }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        Ok((env, fingerprint))
    }

    fn build_request_id_commit_envelopes(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        entries: Vec<crate::port::CommitTransitionEntry>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(Vec<CommandEnvelope>, BodyHash)> {
        if !self.is_atomic() {
            return Err(EngineError::Unavailable);
        }
        // The whole-body fingerprint `commit_transition` stamps on EVERY envelope of this commit, so a
        // post-reopen retry of the same body computes the identical fingerprint → Replay (not Conflict).
        let fingerprint = commit_body_hash(&entries)?;
        let mut g = self.inner.lock().expect("poisoned");
        Self::require_known_shard(&g, shard)?;
        if !g.projection.supports_commit_transition() {
            return Err(EngineError::Unavailable);
        }
        let _ = expected_epoch;
        let commit_fingerprint = fingerprint.0;
        let mut envelopes: Vec<CommandEnvelope> = Vec::new();
        let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
        for entry in entries {
            // Finalize-only restriction (mirrors `build_request_id_commit_envelope`'s single-entry scope): a
            // side-record / lifecycle / instance-fence entry would need the full commit machinery (counter
            // reservation, index validation) this probe deliberately does not replicate.
            if !entry.side_records.is_empty()
                || !entry.lifecycle_items.is_empty()
                || entry.instance_fence.is_some()
            {
                return Err(EngineError::Invalid(
                    "build_request_id_commit_envelopes: finalize-only entries",
                ));
            }
            let claim_ref = entry.claim_ref;
            let consumed_input_id = claim_ref.item_id;
            let additional_claim_refs = entry.additional_claim_refs;
            let additional_consumed_input_ids = additional_claim_refs
                .iter()
                .map(|claim| claim.item_id)
                .collect::<Vec<_>>();
            let mut claim_refs = Vec::with_capacity(1 + additional_claim_refs.len());
            claim_refs.push(claim_ref);
            claim_refs.extend(additional_claim_refs);
            if let Err(error) =
                crate::port::validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..])
            {
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(error),
                });
                continue;
            }
            match g.projection.commit_validate(shard, &claim_refs, now) {
                Ok(()) => {
                    let command_id = Self::next_command_id(&mut g, self.node_id);
                    envelopes.push(CommandEnvelope {
                        command_id,
                        request_id: Some(request_id.clone()),
                        request_fingerprint: Some(commit_fingerprint),
                        request_outcome: None,
                        item_ids: claim_refs.iter().map(|claim| claim.item_id).collect(),
                        command: QueueCommand::Finalize(FinalizeCommand {
                            outcomes: claim_refs
                                .iter()
                                .map(|claim| FinalizeOutcome::new(claim.item_id, entry.finalize))
                                .collect(),
                        }),
                        checksum: CommandChecksum(0),
                        created_at: now,
                    });
                    recovery.push(EntryRecovery {
                        consumed_input_id,
                        additional_consumed_input_ids,
                        instance: None,
                        side_record_keys: Vec::new(),
                        lifecycle_item_ids: Vec::new(),
                        status: CommitEntryStatus::Committed,
                    });
                }
                Err(e) => recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                }),
            }
        }
        // Emit the terminal marker whenever the commit has >=1 REJECTED entry (mixed OR all-rejected),
        // byte-for-byte as `commit_transition` does, so recovery rebuilds the FULL per-entry vec (committed +
        // rejected) from this durable envelope.
        let has_rejected = recovery
            .iter()
            .any(|r| matches!(r.status, CommitEntryStatus::Rejected(_)));
        if has_rejected {
            let outcome_entries: Vec<CommitOutcomeEntry> =
                recovery.iter().map(outcome_entry_from_recovery).collect();
            let command_id = Self::next_command_id(&mut g, self.node_id);
            envelopes.push(CommandEnvelope {
                command_id,
                request_id: Some(request_id.clone()),
                request_fingerprint: Some(commit_fingerprint),
                request_outcome: Some(RequestOutcome::CommitTransition {
                    entries: outcome_entries,
                }),
                item_ids: Vec::new(),
                command: QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                    records: Vec::new(),
                }),
                checksum: CommandChecksum(0),
                created_at: now,
            });
        }
        Ok((envelopes, fingerprint))
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

        Box::pin(async move {
            let permit = self
                .mutation_gate
                .acquire(req.shard.clone())
                .await
                .map_err(|_| EngineError::Unavailable)?;
            let result = (|| {
                // Resolve the claim unit from the compatibility options. Item-level (the default) is the unchanged
                // hot path; a non-item unit (whole-group / same-group-key / whole-cohort) is delegated to the
                // projection axis' rich-claim selection (BQ-14b/c). A projection without a group/cohort read model
                // (the log-replay family) refuses the non-item units with `Unavailable` via `select_rich_claim`.
                let def = self.control.queue_definition(&req.shard)?;
                // The durable control-plane definition can become visible before this handle finishes
                // create-loser hydration. Until atomic projection installation publishes `known_shards`, a
                // claim must fail closed rather than mistake an absent/partial image for an empty queue.
                if !self
                    .inner
                    .lock()
                    .expect("poisoned")
                    .known_shards
                    .contains(&req.shard)
                {
                    return Err(EngineError::NotFound);
                }
                let unit = if req.compatibility != ClaimCompatibility::default() {
                    validate_claim_compatibility(&req.compatibility, req.max_items as u64, &def)?
                } else {
                    ClaimUnit::Item
                };
                if !matches!(unit, ClaimUnit::Item) {
                    return self.claim_rich(&req, unit).map(ClaimStart::Ready);
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
                // Selection runs at the caller-resolved eligibility epoch (`eligibility_time`, defaulting to
                // `now`); the lease/command stamping below stays on `req.now`, so a claim can select work due at
                // one epoch while leasing it against the operational clock.
                let eligibility_at = req.eligibility_at();
                let candidates: Vec<ItemId> = if gc && strict_candidate_cursor {
                    let after = g
                        .coords
                        .get(&req.shard)
                        .and_then(|coord| coord.in_flight_claim_tail);
                    g.projection.eligible_candidates_after(
                        &req.shard,
                        eligibility_at,
                        after,
                        req.max_items,
                    )?
                } else {
                    let in_flight_claims = g
                        .coords
                        .get(&req.shard)
                        .map(|coord| coord.in_flight_claims.clone())
                        .unwrap_or_default();
                    let candidate_limit =
                        req.max_items.saturating_add(in_flight_claims.len()).max(1);
                    g.projection
                        .eligible_candidates(&req.shard, eligibility_at, candidate_limit)?
                        .into_iter()
                        .filter(|id| !in_flight_claims.contains(id))
                        .take(req.max_items)
                        .collect()
                };
                if candidates.is_empty() {
                    return Ok(ClaimStart::Ready(Claimed::default()));
                }
                // TD-004 hard-debt admission gate: a claim that will lease candidates commits a new `Claim`
                // command into the async-apply backlog — new work that adds debt — so it fails CLOSED under Hard
                // backpressure, exactly like a push. Gated only here (candidates non-empty), so an empty claim
                // that commits nothing is never rejected. No-op unless the projection is a hard-backpressured
                // `objectlog/hybrid-async` store.
                g.projection.admit_mutation(&req.shard)?;
                let env = Self::make_envelope(
                    &mut g,
                    self.node_id,
                    QueueCommand::Claim(ClaimCommand {
                        item_ids: candidates.clone(),
                        lease_token: req.lease_token.clone(),
                        lease_expires_at: req.lease_expires_at,
                        worker_id: Some(req.worker_id.clone()),
                    }),
                    candidates.clone(),
                    req.now,
                );
                if gc {
                    let coord = g.coords.entry(req.shard.clone()).or_default();
                    coord.in_flight_claims.extend(candidates.iter().copied());
                    coord.in_flight_claim_tail = candidates.last().copied();
                    let slot =
                        Self::gc_buffer(&mut g, &req.shard, env, req.expected_epoch, req.now)?;
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
            drop(permit);
            match result {
                Ok(ClaimStart::Wait {
                    slot,
                    shard,
                    candidates,
                }) => {
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
                }
                Ok(ClaimStart::Ready(claimed)) => Ok(claimed),
                Err(e) => Err(e),
            }
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
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
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
        })
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
        Box::pin(async move {
            let permit = self
                .mutation_gate
                .acquire(shard.clone())
                .await
                .map_err(|_| EngineError::Unavailable)?;
            let result = (|| {
                let mut g = self.inner.lock().expect("poisoned");
                Self::require_known_shard(&g, shard)?;
                let gc = self.gc_active(&g);
                g.projection.finalize_validate(shard, &outcomes)?;
                let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
                let outcomes = outcomes
                    .into_iter()
                    .map(|mut outcome| {
                        // The legacy synchronous projection boundary does not return the persisted per-item
                        // retry bound. Leave Retry unsealed so the authoritative apply transaction resolves it
                        // from `retry_count` + `max_attempts`; native-async composition seals both values from
                        // one typed validation row before append.
                        outcome.applied_state = match outcome.kind {
                            FinalizeKind::Complete => Some(ItemState::Complete),
                            FinalizeKind::Fail => Some(ItemState::Failed),
                            FinalizeKind::Retry => None,
                            FinalizeKind::Release | FinalizeKind::Rearm => Some(ItemState::Pending),
                        };
                        outcome
                    })
                    .collect::<Vec<_>>();
                let env = Self::make_envelope(
                    &mut g,
                    self.node_id,
                    QueueCommand::Finalize(FinalizeCommand { outcomes }),
                    item_ids,
                    now,
                );
                if gc {
                    Self::gc_buffer(&mut g, shard, env, expected_epoch, now).map(Some)
                } else {
                    Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                    Ok(None)
                }
            })()?;
            drop(permit);
            if let Some(slot) = result {
                SealWait::new(slot).await?;
            }
            Ok(())
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
                item_ids.clone(),
                now,
            );
            if gc {
                Self::gc_commit_sync(&mut g, shard, env, expected_epoch)?;
            } else {
                Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            }
            let renewed: HashSet<ItemId> = item_ids.iter().copied().collect();
            g.claim_by_query_idempotency
                .entry(shard.clone())
                .or_default()
                .extend_expiry_matching(new_lease_expires_at, |(claimed, _)| {
                    !claimed.is_empty() && claimed.iter().all(|item_id| renewed.contains(item_id))
                });
            Ok(())
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            // In-place field/payload merge is an atomic-class feature; an eventual-apply log refuses it.
            if !self.is_atomic() {
                return Err(EngineError::Unavailable);
            }
            validate_api001_reserved_write_fields(&field_ops)?;
            let def = self.control.queue_definition(shard)?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                vec![item_id],
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            g.projection
                .item_version(shard, &item_id)?
                .ok_or(EngineError::NotFound)
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
        })
    }
}

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> ReclaimDriver for ComposedBackend<L, P, C> {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        Box::pin(async move {
            let (definition_page, mut report) = {
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
                let expired_page = g.projection.expired_leases_page(
                    now,
                    g.expired_lease_cursor.as_ref(),
                    DEFINITION_PAGE_LIMIT,
                    self.worker_partition,
                )?;
                let mut projection_catalog = g.maintenance_catalog_projection;
                let catalog_cursor = g.maintenance_definition_cursor.clone();
                let mut definition_page = if projection_catalog {
                    g.projection.recover_definitions_page(
                        catalog_cursor.as_ref(),
                        DEFINITION_PAGE_LIMIT,
                        self.worker_partition,
                    )?
                } else {
                    g.log.recover_definitions_page(
                        catalog_cursor.as_ref(),
                        DEFINITION_PAGE_LIMIT,
                        self.worker_partition,
                    )?
                };
                // An axis with no catalog is common (e.g. composed sqlite/object-log definitions live in the
                // log). Cross that exhausted empty axis in the same tick; a foreign-only page has `next` and is
                // deliberately persisted for the next bounded tick instead.
                if definition_page.definitions.is_empty() && definition_page.next.is_none() {
                    projection_catalog = !projection_catalog;
                    definition_page = if projection_catalog {
                        g.projection.recover_definitions_page(
                            None,
                            DEFINITION_PAGE_LIMIT,
                            self.worker_partition,
                        )?
                    } else {
                        g.log.recover_definitions_page(
                            None,
                            DEFINITION_PAGE_LIMIT,
                            self.worker_partition,
                        )?
                    };
                }
                match definition_page.next.clone() {
                    Some(next) => g.maintenance_definition_cursor = Some(next),
                    None if projection_catalog => {
                        g.maintenance_catalog_projection = false;
                        g.maintenance_definition_cursor = None;
                    }
                    None => {
                        g.maintenance_catalog_projection = true;
                        g.maintenance_definition_cursor = None;
                    }
                }
                let mut report = TickReport::default();
                for (shard, ids) in expired_page.leases {
                    if !g.known_shards.contains(&shard) {
                        continue;
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
                        Self::gc_commit_sync(&mut g, &shard, env, None)?;
                    } else {
                        Self::commit_locked(&mut g, &shard, env, None)?;
                    }
                    report.leases_reclaimed += ids.len() as u64;
                }
                g.expired_lease_cursor = expired_page.next;
                (definition_page, report)
            };

            for def in definition_page.definitions {
                let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
                if !self.owns_worker_queue(&shard) {
                    continue;
                }
                // The paged catalog bounds one tick's queue count. The queue permit spans detached provider
                // execution so local same-queue authority stays stable while unrelated queues keep moving.
                let _queue_permit = self
                    .mutation_gate
                    .acquire(shard.clone())
                    .await
                    .map_err(|_| EngineError::Unavailable)?;
                let frontier_missing = {
                    let mut g = self.inner.lock().expect("poisoned");
                    if !g.known_shards.contains(&shard) {
                        continue;
                    }
                    if def.emit_change_records {
                        Self::reap_terminal_items_locked(
                            &mut g,
                            &shard,
                            now,
                            def.terminal_retention_ms,
                            def.emit_change_records,
                        )?;
                    }
                    g.projection.requires_complete_retention_frontier()
                        && !g.projection.complete_retention_frontier_is_proven(&shard)
                };
                if frontier_missing {
                    report.maintenance.retained += 1;
                    report.maintenance.stopped_by =
                        Some(MaintenanceStopReason::FrontierProofMissing);
                } else {
                    let maintenance = self.trim_reclaimable_segments_detached(
                        &shard,
                        def.request_id_retention_ms
                            .max(def.client_item_key_retention_ms),
                        now,
                    )?;
                    report.maintenance.merge(maintenance);
                }

                let owner_epoch = {
                    let g = self.inner.lock().expect("poisoned");
                    g.log.maintenance_owner_epoch(&shard)
                };
                if let Some(handle) = self.detached_maintenance.as_ref() {
                    let Some(expected_epoch) = owner_epoch else {
                        report.maintenance.retained += 1;
                        report.maintenance.stopped_by =
                            Some(MaintenanceStopReason::OwnershipUnproven);
                        continue;
                    };
                    let maintenance =
                        handle.execute_orphan_gc(&shard, expected_epoch, ts_to_ms(now))?;
                    // Revalidate the local owner token after detached provider I/O. The substrate fenced every
                    // delete; a raced token only prevents us from treating the page as current progress.
                    let still_owned = self
                        .inner
                        .lock()
                        .expect("poisoned")
                        .log
                        .maintenance_owner_epoch(&shard)
                        == Some(expected_epoch);
                    if still_owned {
                        report.maintenance.merge(maintenance);
                    } else {
                        report.maintenance.fenced = true;
                        report.maintenance.stopped_by = Some(MaintenanceStopReason::EpochFenced);
                    }
                } else {
                    let mut g = self.inner.lock().expect("poisoned");
                    if !g.log.supports_objectlog_maintenance() {
                        continue;
                    }
                    let Some(expected_epoch) = owner_epoch else {
                        report.maintenance.retained += 1;
                        report.maintenance.stopped_by =
                            Some(MaintenanceStopReason::OwnershipUnproven);
                        continue;
                    };
                    let maintenance = g.log.gc_orphaned_branches_bounded(
                        &shard,
                        expected_epoch,
                        ts_to_ms(now),
                    )?;
                    report.maintenance.merge(maintenance);
                }
            }
            Ok(report)
        })
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
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .log
                .read_from(shard, from, limit)
        })
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
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .select_eligible(shard, now, limit)
        })
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .peek(shard, limit)
        })
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .pending(shard)
        })
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .pending_summary(shard)
        })
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .pending_page(shard, start, limit)
        })
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .pending_range(shard, start, end, consumer, limit)
        })
    }

    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .pending_by_ids(shard, ids)
        })
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .render_claimed(shard, ids)
        })
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .live_items(shard, keys)
        })
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .metrics(queue)
        })
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .terminal_emission_metrics(shard, now, emit_change_records, emission_cursor)
        })
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
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .log
                .current_position(shard)
        })
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
        deferred(move || {
            let g = self.inner.lock().expect("poisoned");
            // Relational (no-replayable-log) projection stores cannot reconstruct historical state.
            // Decline as-of reads with `Unavailable` up-front, before the queue-existence lookup, so the
            // composed relational backend matches the monolithic relational backends (which return
            // `Unavailable` regardless of whether the queue exists). Log-replay stores return `true` here
            // and fall through to the normal existence check + replay path below.
            if !g.projection.supports_as_of() {
                return Err(EngineError::Unavailable);
            }
            let definition = self.control.queue_definition(shard)?;
            let snapshot_ref = g.log.snapshot_at_or_before(shard, &position)?;
            let snapshot = match snapshot_ref.as_ref() {
                Some(snapshot_ref) => Some(g.log.read_snapshot(snapshot_ref)?),
                None => None,
            };
            let mut as_of = g.projection.reconstruct_as_of(&definition, snapshot)?;
            let mut from = snapshot_ref.map(|s| s.position);
            loop {
                let page = g
                    .log
                    .read_from(shard, from.clone(), RECOVERY_READ_PAGE_LIMIT)?;
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
        })
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
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .index_get_unique(shard, index, key)
        })
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .index_lookup(shard, index, key)
        })
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
        deferred(move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
            g.log.write_snapshot(shard, position, snapshot)
        })
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .log
                .latest_snapshot(shard)
        })
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .log
                .read_snapshot(snapshot_ref)
        })
    }

    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        deferred(move || self.inner.lock().expect("poisoned").log.high_water(shard))
    }

    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        deferred(move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
            g.log.set_high_water(shard, position)
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            // Reschedule is an atomic-class feature; an eventual-apply log refuses it (no eligibility re-key).
            if !self.is_atomic() {
                return Err(EngineError::Unavailable);
            }
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
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
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                vec![item_id],
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            g.projection
                .item_version(shard, &item_id)?
                .ok_or(EngineError::NotFound)
        })
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
        self.inner
            .lock()
            .expect("poisoned")
            .projection
            .hot_projection_capabilities()
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<RangeScanResponse>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .range_scan(shard, request)
        })
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .grouped_aggregate(shard, request)
        })
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .metrics_by_query(shard, request)
        })
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .declared_bucket_segment(shard, request)
        })
    }

    fn bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
    ) -> impl std::future::Future<Output = EngineResult<BoundedMutationResponse>> + Send {
        deferred(move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
            g.projection.bounded_mutation(shard, request)
        })
    }

    fn claim_by_query(
        &self,
        shard: &QueueKey,
        request: ClaimByQueryRequest,
        context: crate::port::ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            let definition = self.control.queue_definition(shard)?;
            if request.max_items == 0
                || u64::from(request.max_items) > definition.max_claim_batch_size
            {
                return Err(EngineError::Invalid("invalid claim_by_query max_items"));
            }
            if request.lease_duration_ms == 0
                || request.lease_duration_ms > definition.max_lease_duration_ms
            {
                return Err(EngineError::Invalid(
                    "invalid claim_by_query lease_duration_ms",
                ));
            }
            let request_id = request
                .request_id
                .clone()
                .ok_or(EngineError::Invalid("claim_by_query request_id required"))?;
            let fingerprint = claim_by_query_body_hash(&request)?;
            let expires_at = request_expires_at(context.now, definition.request_id_retention_ms);
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
            match g
                .claim_by_query_idempotency
                .entry(shard.clone())
                .or_default()
                .check_conflict_first(&request_id, fingerprint, context.now)
            {
                IdempotencyDecision::Replay((item_ids, lease_token)) => {
                    let items = g.projection.render_claimed(shard, &item_ids)?;
                    if items.len() != item_ids.len()
                        || items
                            .iter()
                            .any(|item| item.lease_expires_at <= context.now)
                    {
                        return Err(EngineError::RequestExpired);
                    }
                    for item in &items {
                        if item.lease_token.as_ref() != Some(&lease_token) {
                            return Err(EngineError::RequestExpired);
                        }
                    }
                    return Ok(Claimed {
                        items,
                        ..Default::default()
                    });
                }
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Expired => return Err(EngineError::RequestExpired),
                IdempotencyDecision::Proceed => {}
            }
            let eligible: HashSet<ItemId> = g
                .projection
                .eligible_candidates(shard, context.eligibility_at(), usize::MAX)?
                .into_iter()
                .collect();
            let reserved = g.coords.get(shard).map(|coord| &coord.in_flight_claims);
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
                item_ids.extend(
                    page.rows
                        .into_iter()
                        .map(|row| row.item_id)
                        .filter(|item_id| {
                            eligible.contains(item_id)
                                && reserved.is_none_or(|reserved| !reserved.contains(item_id))
                        }),
                );
                item_ids.truncate(request.max_items as usize);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }

            if item_ids.is_empty() {
                let lease_token = LeaseToken::new("empty-claim").expect("valid token");
                let command_id = Self::next_command_id(&mut g, self.node_id);
                let env = CommandEnvelope {
                    command_id,
                    request_id: Some(request_id.clone()),
                    request_fingerprint: Some(fingerprint.0),
                    request_outcome: Some(RequestOutcome::ClaimByQuery {
                        item_ids: Vec::new(),
                        lease_token: lease_token.clone(),
                        worker_id: Some(request.worker_id.clone()),
                    }),
                    item_ids: Vec::new(),
                    command: QueueCommand::Claim(ClaimCommand {
                        item_ids: Vec::new(),
                        lease_token: lease_token.clone(),
                        lease_expires_at: context.lease_expires_at(request.lease_duration_ms),
                        worker_id: Some(request.worker_id.clone()),
                    }),
                    checksum: CommandChecksum(0),
                    created_at: context.now,
                };
                Self::commit_locked(&mut g, shard, env, None)?;
                g.claim_by_query_idempotency
                    .entry(shard.clone())
                    .or_default()
                    .record(
                        request_id,
                        fingerprint,
                        (Vec::new(), lease_token),
                        expires_at,
                    );
                return Ok(Claimed::default());
            }

            let created_at = context.now;
            let lease_expires_at = context.lease_expires_at(request.lease_duration_ms);
            let lease_token = generate_query_lease_token()?;
            let command_id = Self::next_command_id(&mut g, self.node_id);
            let env = CommandEnvelope {
                command_id,
                request_id: Some(request_id.clone()),
                request_fingerprint: Some(fingerprint.0),
                request_outcome: Some(RequestOutcome::ClaimByQuery {
                    item_ids: item_ids.clone(),
                    lease_token: lease_token.clone(),
                    worker_id: Some(request.worker_id.clone()),
                }),
                item_ids: item_ids.clone(),
                command: QueueCommand::Claim(ClaimCommand {
                    item_ids: item_ids.clone(),
                    lease_token: lease_token.clone(),
                    lease_expires_at,
                    worker_id: Some(request.worker_id.clone()),
                }),
                checksum: CommandChecksum(0),
                created_at,
            };
            Self::commit_locked(&mut g, shard, env, None)?;
            let items = g.projection.render_claimed(shard, &item_ids)?;
            debug_assert_eq!(
                items.len(),
                item_ids.len(),
                "every queried claim candidate must render"
            );
            let replay_expires_at = expires_at.max(lease_expires_at);
            g.claim_by_query_idempotency
                .entry(shard.clone())
                .or_default()
                .record(
                    request_id,
                    fingerprint,
                    (item_ids, lease_token),
                    replay_expires_at,
                );
            Ok(Claimed {
                items,
                ..Default::default()
            })
        })
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
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
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
            Self::require_known_shard(&g, shard)?;
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
                        // FAITHFULNESS GUARD (safety net for pre-marker logs). An in-process record, and a
                        // record rebuilt across restart from the durable `CommitTransition` marker, both hold
                        // ONE `EntryRecovery` per input entry (committed AND rejected), so their length equals
                        // the resubmitted body's entry count and the replay is faithful — including for a MIXED
                        // commit (bead pqueue-db60657d). This guard only bites on a log written BEFORE the
                        // marker existed: there a mixed commit rebuilds to just its COMMITTED,
                        // `Finalize`-delimited entries (a rejected entry appended nothing to reconstruct), a
                        // SHORTER vec. Do NOT return that misleading short outcome vec; fall through to safe
                        // re-execution (the already-committed inputs stay finalized exactly once — commit_validate
                        // fences them — so re-execution is 0-duplicate, and the end-of-call record overwrites the
                        // stale short entry with the full re-executed vec).
                        if recovery.len() == entries.len() {
                            return Ok(outcomes_from_recovery(&recovery));
                        }
                    }
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }

            // (2) Per entry: validate the lease-token + version-fenced claim_ref AND the optional instance
            //     fence, then BUILD (but do not yet append) the entry's side-records + fence advance +
            //     lifecycle push + input finalize. A rejected entry mutates nothing. The whole commit — every
            //     committed entry's envelopes AND the outcome marker (below) — is appended as ONE atomic log
            //     batch at the end, so a crash can never leave a HALF state (committed entries durable but the
            //     outcome record not, or vice-versa; bead pqueue-db60657d Problem 1).
            //
            //     Because the durable append is deferred to the end, a committed entry is NOT applied to the
            //     projection before the next entry validates, so we thread the in-commit effects that a later
            //     entry could observe through lightweight overlays: `finalized_in_commit` (an input already
            //     finalized by a prior entry is no longer a live claim), `staged_fences` (a prior entry may
            //     have advanced an instance fence this entry chains onto), and `committed_pushes` (a prior
            //     entry's lifecycle push occupies unique-index keys). These make the deferred-append path
            //     BYTE-IDENTICAL to the old per-entry apply for the realistic disjoint-entry commit AND prevent
            //     a batched apply from ever double-finalizing (an apply error) or double-inserting a unique key
            //     (silent overwrite) when two entries touch the same input/index within one commit.
            let commit_fingerprint = fingerprint.0;
            let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
            let mut committed_envelopes: Vec<CommandEnvelope> = Vec::new();
            let mut finalized_in_commit: HashSet<ItemId> = HashSet::new();
            let mut staged_fences: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut committed_pushes: Vec<PushItem> = Vec::new();
            for entry in entries {
                let claim_ref = entry.claim_ref;
                let consumed_input_id = claim_ref.item_id;
                let additional_claim_refs = entry.additional_claim_refs;
                let additional_consumed_input_ids = additional_claim_refs
                    .iter()
                    .map(|claim| claim.item_id)
                    .collect::<Vec<_>>();
                let mut claim_refs = Vec::with_capacity(1 + additional_claim_refs.len());
                claim_refs.push(claim_ref);
                claim_refs.extend(additional_claim_refs);
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids: additional_consumed_input_ids.clone(),
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };

                // In-commit duplicate-input guard: a prior entry in THIS commit already finalized this input,
                // so it is no longer a live claim (mirrors the sequential-apply rejection — its lease is gone —
                // and prevents a second Finalize for the same item, which would be an apply error).
                if let Err(error) =
                    crate::port::validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..])
                {
                    recovery.push(reject(error));
                    continue;
                }
                if claim_refs
                    .iter()
                    .any(|claim| finalized_in_commit.contains(&claim.item_id))
                {
                    recovery.push(reject(EngineError::Terminal));
                    continue;
                }
                if let Err(e) = g.projection.commit_validate(shard, &claim_refs, now) {
                    recovery.push(reject(e));
                    continue;
                }

                // C6: validate the caller-supplied instance fence against the stored fence (absent == 0),
                // reading through the in-commit `staged_fences` overlay so a fence a prior entry advanced is
                // visible here exactly as sequential apply would have made it.
                if let Some(fence) = &entry.instance_fence {
                    let stored = match staged_fences.get(&fence.instance_key) {
                        Some(v) => *v,
                        None => g
                            .projection
                            .instance_fence(shard, &fence.instance_key)?
                            .unwrap_or(0),
                    };
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
                // AND the whole-body fingerprint propagate into every envelope: the request_id is the
                // idempotency key and the fingerprint is what `check` compares for replay-vs-conflict, so
                // stamping both durably (in the SAME pre-existing envelope fields the push path uses — no
                // wire-format change, no new serialization) is exactly what lets recovery rebuild the
                // `commit_idempotency` cache from the log (`rebuild_idempotency_from_log`) so a
                // post-restart request_id retry replays the one committed result instead of re-executing.
                let mut envelopes: Vec<CommandEnvelope> = Vec::new();
                let mk_env = |g: &mut Inner<L, P>, command: QueueCommand, item_ids: Vec<ItemId>| {
                    let command_id = Self::next_command_id(g, self.node_id);
                    CommandEnvelope {
                        command_id,
                        request_id: request_id.clone(),
                        request_fingerprint: Some(commit_fingerprint),
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
                let mut entry_pushes: Vec<PushItem> = Vec::new();
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
                    // Index-validate against the projection AND the pushes prior committed entries in THIS
                    // commit already claimed (they are not yet applied), so a unique-key collision between two
                    // entries of the same commit rejects here exactly as sequential apply would have.
                    let mut candidate = committed_pushes.clone();
                    candidate.extend(push_items.iter().cloned());
                    if let Err(e) = g.projection.index_validate_push(shard, &candidate) {
                        recovery.push(reject(e));
                        continue;
                    }
                    lifecycle_item_ids = ids.clone();
                    entry_pushes = push_items.clone();
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
                        outcomes: claim_refs
                            .iter()
                            .map(|claim| FinalizeOutcome::new(claim.item_id, entry.finalize))
                            .collect(),
                    }),
                    claim_refs.iter().map(|claim| claim.item_id).collect(),
                );
                envelopes.push(e);

                // Accept: fold this committed entry's effects into the in-commit overlays and collect its
                // envelopes for the single atomic append below (NOT appended yet).
                finalized_in_commit.extend(claim_refs.iter().map(|claim| claim.item_id));
                if let Some((key, next)) = &instance {
                    staged_fences.insert(key.clone(), *next);
                }
                committed_pushes.extend(entry_pushes);
                committed_envelopes.append(&mut envelopes);
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }

            // (3) Durably record the FULL per-entry outcome (committed AND rejected) for ANY request_id-bearing
            //     commit that has >=1 REJECTED entry — MIXED and ALL-REJECTED alike (bead pqueue-db60657d
            //     Problems 1 & 2). A rejected entry mutates and appends nothing of its own, so without this its
            //     outcome is lost across a restart: a MIXED commit would rebuild a SHORTER vec (falling through
            //     the `recovery.len() == entries.len()` guard to re-execution), and an ALL-REJECTED commit would
            //     rebuild NOTHING and re-execute — and re-execution reads `now`, so a Conflict rejected before a
            //     lease expired can re-reject StaleLease after it (a DIFFERENT structured error, not the
            //     byte-identical replay the live in-memory record gives). We stamp the whole vec on ONE terminal
            //     marker envelope (a no-op empty `WriteSideRecords`) carrying the caller's request_id +
            //     whole-body fingerprint + `RequestOutcome::CommitTransition`. An ALL-COMMITTED commit needs no
            //     marker (recovery reconstructs it exactly from its `Finalize` runs). The marker rides in the
            //     SAME atomic append as the committed envelopes, so the outcome is durable EXACTLY when the
            //     commit is — never a half state.
            let mut batch = committed_envelopes;
            if let Some(rid) = &request_id
                && recovery
                    .iter()
                    .any(|r| matches!(r.status, CommitEntryStatus::Rejected(_)))
            {
                let outcome_entries: Vec<CommitOutcomeEntry> =
                    recovery.iter().map(outcome_entry_from_recovery).collect();
                let command_id = Self::next_command_id(&mut g, self.node_id);
                batch.push(CommandEnvelope {
                    command_id,
                    request_id: Some(rid.clone()),
                    request_fingerprint: Some(commit_fingerprint),
                    request_outcome: Some(RequestOutcome::CommitTransition {
                        entries: outcome_entries,
                    }),
                    item_ids: Vec::new(),
                    command: QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                        records: Vec::new(),
                    }),
                    checksum: CommandChecksum(0),
                    created_at: now,
                });
            }
            // ONE atomic append+apply for the WHOLE commit (all committed entries' envelopes + the outcome
            // marker). The epoch cannot change while we hold the lock, so either this fences (EpochFenced,
            // before any mutation) or the whole commit's writes commit and apply together — no crash window
            // between committed-entry durability and outcome durability.
            if !batch.is_empty() {
                Self::commit_locked_batch(&mut g, shard, batch, expected_epoch)?;
            }

            // (4) Record the whole-body recovery only AFTER success, so a later replay/explain returns it
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
        })
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
        deferred(move || {
            let g = self.inner.lock().expect("poisoned");
            if !self.is_atomic() || !g.projection.supports_commit_transition() {
                Err(EngineError::Unavailable)
            } else {
                Ok(g.commit_idempotency
                    .get(shard)
                    .and_then(|c| c.peek(&request_id))
                    .map(|entries| CommitRecovery {
                        request_id,
                        entries,
                    }))
            }
        })
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .side_record(shard, key)
        })
    }
}

// ---------------------------------------------------------------------------
// Capability-delegating ports (relational-class features). Gate state (`SetGates`) and per-group
// active-scope discovery are projection-axis capabilities: the relational projection materializes the gate
// tables + per-group summary and implements them, while the in-memory / log-replay family stores neither
// and refuses them via the projection defaults — exact capability parity with the monolithic `MemoryBackend`
// and `SqliteRelationalBackend`.
// ---------------------------------------------------------------------------

impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::SetGatesPort
    for ComposedBackend<L, P, C>
{
    /// Operator gate-state mutation. Committed through the atomic write seam as a `SetGates` command; its
    /// apply arm sets/clears the projection's gate-state rows (exact-on-read: the next claim's eligibility
    /// anti-join sees the change). A non-gate projection refuses it at `commit_locked`'s gate validation
    /// (`Unavailable`), so the log-replay family rejects it unchanged.
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        queue_serialized(&self.mutation_gate, shard.clone(), move || {
            let mut g = self.inner.lock().expect("poisoned");
            Self::require_known_shard(&g, shard)?;
            let env = Self::make_envelope(
                &mut g,
                self.node_id,
                QueueCommand::SetGates(command),
                Vec::new(),
                now,
            );
            Self::commit_locked(&mut g, shard, env, expected_epoch)
        })
    }
}
impl<L: LogStore, P: ProjectionStore, C: ControlPlane> crate::port::DiscoveryPort
    for ComposedBackend<L, P, C>
{
    /// Per-group active-scope discovery (BQ-14e) — delegates to the projection axis' summary rollup. A
    /// projection with no per-group summary refuses with `Unavailable` (log-replay family), preserving parity.
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: crate::active_scope::DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::active_scope::ActiveScope>>> + Send
    {
        deferred(move || {
            self.inner
                .lock()
                .expect("poisoned")
                .projection
                .discover_active_scopes(shard, granularity, now)
        })
    }
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
    use crate::PauseQueueCommand;
    use crate::port::{ClaimPort, ControlPlaneStore, ProjectionRead, PushPort, ReclaimDriver};
    use crate::{LogStore, QueueMetrics};
    use fireweed_core::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
        PriorityTieBreaker, RecurrencePolicy, RetryPolicy, TenantId, WorkerId,
    };
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::task::Poll;

    #[derive(Clone, Default)]
    struct FakeLogState {
        epoch: u64,
        high_water: Option<CommandPosition>,
        snapshots: Vec<(SnapshotRef, ProjectionSnapshot)>,
        next_sequence: u64,
        buffered: Vec<CommandEnvelope>,
        sealed_batches: Vec<usize>,
        entries: Vec<(CommandPosition, CommandEnvelope)>,
        emission_cursor: HashMap<QueueKey, CommandPosition>,
        definitions: BTreeMap<QueueKey, QueueDefinition>,
        cursor_write_failures: u32,
        read_page_limit: Option<usize>,
        fail_read_call: Option<usize>,
    }

    #[derive(Default)]
    struct FakeGroupCommitLog {
        state: Mutex<FakeLogState>,
        read_calls: AtomicUsize,
        catalog_page_calls: AtomicUsize,
    }

    impl Clone for FakeGroupCommitLog {
        fn clone(&self) -> Self {
            Self {
                state: Mutex::new(self.state.lock().expect("fake log poisoned").clone()),
                read_calls: AtomicUsize::new(self.read_calls.load(AtomicOrdering::Relaxed)),
                catalog_page_calls: AtomicUsize::new(
                    self.catalog_page_calls.load(AtomicOrdering::Relaxed),
                ),
            }
        }
    }

    impl FakeGroupCommitLog {
        fn sealed_batches(&self) -> Vec<usize> {
            self.state
                .lock()
                .expect("fake log poisoned")
                .sealed_batches
                .clone()
        }

        fn entry_count(&self) -> usize {
            self.state.lock().expect("fake log poisoned").entries.len()
        }

        fn read_calls(&self) -> usize {
            self.read_calls.load(AtomicOrdering::Relaxed)
        }

        fn set_entries(
            &self,
            shard: &QueueKey,
            epoch: u64,
            entries: Vec<CommandEnvelope>,
        ) -> Vec<CommandPosition> {
            let mut state = self.state.lock().expect("fake log poisoned");
            state.epoch = epoch;
            state.next_sequence = 0;
            state.entries.clear();
            entries
                .into_iter()
                .map(|env| {
                    let position =
                        CommandPosition::new(shard.clone(), state.epoch, state.next_sequence);
                    state.next_sequence += 1;
                    state.entries.push((position.clone(), env));
                    position
                })
                .collect::<Vec<_>>()
        }

        fn fail_next_cursor_write(&self) {
            self.state
                .lock()
                .expect("fake log poisoned")
                .cursor_write_failures += 1;
        }

        fn page_reads_at_most(&self, limit: usize) {
            self.state
                .lock()
                .expect("fake log poisoned")
                .read_page_limit = Some(limit);
        }

        fn fail_read_call_once(&self, call: usize) {
            self.state.lock().expect("fake log poisoned").fail_read_call = Some(call);
        }

        fn record_definition(&self, definition: &QueueDefinition) {
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.state
                .lock()
                .expect("fake log poisoned")
                .definitions
                .insert(key, definition.clone());
        }

        fn seal_buffered(state: &mut FakeLogState, shard: &QueueKey) -> Vec<CommandPosition> {
            let n = state.buffered.len();
            let mut positions = Vec::with_capacity(n);
            for env in state.buffered.drain(..) {
                let p = CommandPosition::new(shard.clone(), state.epoch, state.next_sequence);
                state.next_sequence += 1;
                state.entries.push((p.clone(), env));
                positions.push(p);
            }
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
            let mut positions = Vec::with_capacity(commands.len());
            for env in commands {
                let p = CommandPosition::new(shard.clone(), state.epoch, state.next_sequence);
                state.next_sequence += 1;
                state.entries.push((p.clone(), env.clone()));
                positions.push(p);
            }
            state.sealed_batches.push(commands.len());
            Ok(positions)
        }

        fn read_from(
            &self,
            shard: &QueueKey,
            from: Option<CommandPosition>,
            limit: usize,
        ) -> EngineResult<CommandPage> {
            let call = self.read_calls.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            let mut state = self.state.lock().expect("fake log poisoned");
            if state.fail_read_call == Some(call) {
                state.fail_read_call = None;
                return Err(EngineError::Storage(
                    "fault-injection: later replay page read failed".into(),
                ));
            }
            let start = match from.as_ref() {
                None => 0,
                Some(cursor) => state
                    .entries
                    .iter()
                    .position(|(position, _)| position == cursor)
                    .map_or_else(
                        || {
                            state
                                .entries
                                .iter()
                                .position(|(position, _)| cursor.precedes(position))
                                .unwrap_or(state.entries.len())
                        },
                        |idx| idx + 1,
                    ),
            };
            let page_limit = state
                .read_page_limit
                .map_or(limit, |configured| limit.min(configured))
                .max(1);
            let entries = state
                .entries
                .iter()
                .skip(start)
                .take(page_limit)
                .cloned()
                .collect::<Vec<_>>();
            let next = if start + entries.len() < state.entries.len() {
                entries.last().map(|(position, _)| {
                    CommandPosition::new(shard.clone(), position.backend_epoch, position.sequence)
                })
            } else {
                None
            };
            Ok(CommandPage { entries, next })
        }

        fn high_water(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
            Ok(self
                .state
                .lock()
                .expect("fake log poisoned")
                .high_water
                .clone())
        }

        fn set_high_water(
            &mut self,
            _shard: &QueueKey,
            position: CommandPosition,
        ) -> EngineResult<()> {
            self.state.get_mut().expect("fake log poisoned").high_water = Some(position);
            Ok(())
        }

        fn persist_definition(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
            self.record_definition(definition);
            Ok(())
        }

        fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
            Ok(self
                .state
                .lock()
                .expect("fake log poisoned")
                .definitions
                .values()
                .cloned()
                .collect())
        }

        fn recover_definitions_page(
            &self,
            cursor: Option<&DefinitionCursor>,
            limit: usize,
            worker_partition: Option<(usize, usize)>,
        ) -> EngineResult<DefinitionPage> {
            self.catalog_page_calls
                .fetch_add(1, AtomicOrdering::Relaxed);
            definition_page_from_iter(
                self.state
                    .lock()
                    .expect("fake log poisoned")
                    .definitions
                    .values()
                    .cloned(),
                cursor,
                limit,
                worker_partition,
            )
        }

        fn emission_cursor(&self, _shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
            Ok(self
                .state
                .lock()
                .expect("fake log poisoned")
                .emission_cursor
                .get(_shard)
                .cloned())
        }

        fn supports_emission_cursor(&self) -> bool {
            true
        }

        fn set_emission_cursor(
            &mut self,
            shard: &QueueKey,
            position: CommandPosition,
        ) -> EngineResult<()> {
            let state = self.state.get_mut().expect("fake log poisoned");
            if state.cursor_write_failures > 0 {
                state.cursor_write_failures -= 1;
                return Err(EngineError::Unavailable);
            }
            state.emission_cursor.insert(shard.clone(), position);
            Ok(())
        }

        fn write_snapshot(
            &mut self,
            shard: &QueueKey,
            position: CommandPosition,
            snapshot: ProjectionSnapshot,
        ) -> EngineResult<SnapshotRef> {
            let snapshot_ref = SnapshotRef {
                queue: shard.clone(),
                position,
                ref_id: String::from_utf8_lossy(&snapshot.payload).into_owned(),
            };
            self.state
                .get_mut()
                .expect("fake log poisoned")
                .snapshots
                .push((snapshot_ref.clone(), snapshot));
            Ok(snapshot_ref)
        }

        fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
            Ok(self
                .state
                .lock()
                .expect("fake log poisoned")
                .snapshots
                .iter()
                .rev()
                .find(|(snapshot_ref, _)| &snapshot_ref.queue == shard)
                .map(|(snapshot_ref, _)| snapshot_ref.clone()))
        }

        fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
            self.state
                .lock()
                .expect("fake log poisoned")
                .snapshots
                .iter()
                .find(|(candidate, _)| {
                    candidate.queue == snapshot_ref.queue
                        && candidate.position == snapshot_ref.position
                        && candidate.ref_id == snapshot_ref.ref_id
                })
                .map(|(_, snapshot)| snapshot.clone())
                .ok_or(EngineError::NotFound)
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
        lease_expires_at: BTreeMap<ItemId, UtcTimestamp>,
        terminal: HashMap<ItemId, FakeTerminalRecord>,
        paused: bool,
        pause_drain_intake: bool,
        apply_batches: Vec<Vec<&'static str>>,
        reject_admission: bool,
    }

    #[derive(Clone)]
    struct FakeTerminalRecord {
        terminal_at: UtcTimestamp,
        terminal_position: Option<CommandPosition>,
    }

    impl FakeProjection {
        fn apply_batches(&self) -> Vec<Vec<&'static str>> {
            self.state
                .lock()
                .expect("fake projection poisoned")
                .apply_batches
                .clone()
        }

        fn seed_terminal_item(
            &self,
            item_id: ItemId,
            terminal_at: UtcTimestamp,
            terminal_position: Option<CommandPosition>,
        ) {
            self.state
                .lock()
                .expect("fake projection poisoned")
                .terminal
                .insert(
                    item_id,
                    FakeTerminalRecord {
                        terminal_at,
                        terminal_position,
                    },
                );
        }
    }

    impl ProjectionStore for FakeProjection {
        fn ensure_shard(&mut self, _definition: &QueueDefinition) -> EngineResult<()> {
            Ok(())
        }

        fn admit_mutation(&mut self, _shard: &QueueKey) -> EngineResult<()> {
            if self
                .state
                .get_mut()
                .expect("fake projection poisoned")
                .reject_admission
            {
                Err(EngineError::Unavailable)
            } else {
                Ok(())
            }
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
                                state.lease_expires_at.insert(*id, claim.lease_expires_at);
                            }
                        }
                    }
                    QueueCommand::LeaseExpired(c) => {
                        for id in &c.item_ids {
                            state.leased.remove(id);
                            state.lease_expires_at.remove(id);
                            state.pending.push(*id);
                        }
                    }
                    QueueCommand::PauseQueue(c) => {
                        state.paused = true;
                        state.pause_drain_intake = c.drain_intake;
                    }
                    QueueCommand::ResumeQueue => {
                        state.paused = false;
                        state.pause_drain_intake = false;
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        fn install_recovery_shard(
            &mut self,
            definition: &QueueDefinition,
            positions: &[CommandPosition],
            commands: &[CommandEnvelope],
        ) -> EngineResult<()> {
            let mut replacement = Self::default();
            replacement.ensure_shard(definition)?;
            replacement.apply(positions, commands)?;
            // Fake apply is infallible after validation; replacing the test state models the production
            // in-memory projection's scratch-image publication seam.
            *self = replacement;
            Ok(())
        }

        fn pause_blocks_intake(&self, _shard: &QueueKey) -> EngineResult<bool> {
            let state = self.state.lock().expect("fake projection poisoned");
            Ok(state.paused && state.pause_drain_intake)
        }

        fn eligible_candidates(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
            max: usize,
        ) -> EngineResult<Vec<ItemId>> {
            let state = self.state.lock().expect("fake projection poisoned");
            if state.paused {
                return Ok(Vec::new());
            }
            Ok(state.pending.iter().copied().take(max).collect())
        }

        fn eligible_candidates_after(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
            after: Option<ItemId>,
            max: usize,
        ) -> EngineResult<Vec<ItemId>> {
            let state = self.state.lock().expect("fake projection poisoned");
            if state.paused {
                return Ok(Vec::new());
            }
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
                    let lease_expires_at =
                        state.lease_expires_at.get(id).copied().unwrap_or(ts(60));
                    state.leased.get(id).map(|token| ClaimedItem {
                        item_id: *id,
                        client_item_key: ClientItemKey::new(id.to_string()).unwrap(),
                        item_version: 1,
                        priority: None,
                        group_key: None,
                        not_before: None,
                        lease_token: Some(token.clone()),
                        lease_expires_at,
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
            now: UtcTimestamp,
        ) -> EngineResult<Vec<ItemId>> {
            let state = self.state.lock().expect("fake projection poisoned");
            Ok(state
                .lease_expires_at
                .iter()
                .filter_map(|(id, exp)| (*exp < now).then_some(*id))
                .collect())
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
            let resident_terminal_count = self
                .state
                .lock()
                .expect("fake projection poisoned")
                .terminal
                .len() as u64;
            Ok(QueueMetrics {
                pending,
                resident_terminal_count,
                ..Default::default()
            })
        }

        fn terminal_emission_metrics(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
            _emit_change_records: bool,
            _emission_cursor: Option<&CommandPosition>,
        ) -> EngineResult<TerminalEmissionMetrics> {
            let resident_terminal_count = self
                .state
                .lock()
                .expect("fake projection poisoned")
                .terminal
                .len() as u64;
            Ok(TerminalEmissionMetrics {
                resident_terminal_count,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
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

        fn metrics_by_query(
            &self,
            _shard: &QueueKey,
            _request: MetricsByQueryRequest,
        ) -> EngineResult<QueueMetrics> {
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

        fn reap_terminal_items(
            &mut self,
            _shard: &QueueKey,
            now: UtcTimestamp,
            terminal_retention_ms: u64,
            emit_change_records: bool,
            emission_cursor: Option<&CommandPosition>,
        ) -> EngineResult<Vec<ItemId>> {
            let state = self.state.get_mut().expect("fake projection poisoned");
            let mut ids = Vec::new();
            for (id, rec) in &state.terminal {
                if add_millis(rec.terminal_at, terminal_retention_ms) > now {
                    continue;
                }
                if emit_change_records {
                    let Some(terminal_position) = rec.terminal_position.as_ref() else {
                        continue;
                    };
                    let Some(cursor) = emission_cursor else {
                        continue;
                    };
                    if cursor.precedes(terminal_position) {
                        continue;
                    }
                }
                ids.push(*id);
            }
            for id in &ids {
                state.terminal.remove(id);
            }
            Ok(ids)
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
            emit_change_records: true,
        }
    }

    #[test]
    fn pooled_recovery_assigns_each_durable_queue_to_exactly_one_worker() {
        let log = FakeGroupCommitLog::default();
        let tenant = TenantId::new("tenant").unwrap();
        for index in 0..65 {
            let mut definition = qdef();
            definition.queue_id = QueueId::new(format!("queue-{index}")).unwrap();
            log.record_definition(&definition);
        }

        for width in [1, 3, 8] {
            let mut recovered_by = HashMap::<QueueId, usize>::new();
            for worker in 0..width {
                let backend = ComposedBackend::new(
                    log.clone(),
                    FakeProjection::default(),
                    InProcessControlPlane::new(),
                )
                .recover_worker_partition(worker, width)
                .unwrap();
                let mut list = Box::pin(ControlPlaneStore::list_queues(&backend, &tenant));
                let Poll::Ready(Ok(queues)) = poll_once(&mut list) else {
                    panic!("partitioned recovery list must complete")
                };
                for queue_id in queues {
                    assert_eq!(
                        queue_worker_partition(
                            &QueueKey::new(tenant.clone(), queue_id.clone()),
                            width,
                        ),
                        worker
                    );
                    assert!(recovered_by.insert(queue_id, worker).is_none());
                }
            }
            assert_eq!(recovered_by.len(), 65);
        }
    }

    #[test]
    fn durable_catalog_pages_progress_past_an_empty_partition_page() {
        let log = FakeGroupCommitLog::default();
        let tenant = TenantId::new("tenant").unwrap();
        let target_worker = 1;
        let width = 2;
        let mut created = 0usize;
        let mut suffix = 0usize;
        while created < DEFINITION_PAGE_LIMIT {
            let queue_id = QueueId::new(format!("a-{suffix:06}")).unwrap();
            suffix += 1;
            let shard = QueueKey::new(tenant.clone(), queue_id.clone());
            if queue_worker_partition(&shard, width) != target_worker {
                let mut definition = qdef();
                definition.queue_id = queue_id;
                log.record_definition(&definition);
                created += 1;
            }
        }
        let owned = loop {
            let queue_id = QueueId::new(format!("z-{suffix:06}")).unwrap();
            suffix += 1;
            let shard = QueueKey::new(tenant.clone(), queue_id.clone());
            if queue_worker_partition(&shard, width) == target_worker {
                let mut definition = qdef();
                definition.queue_id = queue_id;
                log.record_definition(&definition);
                break definition;
            }
        };

        let first = log
            .recover_definitions_page(None, DEFINITION_PAGE_LIMIT, Some((target_worker, width)))
            .unwrap();
        assert!(first.definitions.is_empty());
        let second = log
            .recover_definitions_page(
                first.next.as_ref(),
                DEFINITION_PAGE_LIMIT,
                Some((target_worker, width)),
            )
            .unwrap();
        assert_eq!(second.definitions, vec![owned]);
        assert!(second.next.is_none());
        assert_eq!(log.catalog_page_calls.load(AtomicOrdering::Relaxed), 2);
    }

    #[test]
    fn hash_map_definition_rows_form_globally_sorted_exactly_once_pages() {
        let mut expected = Vec::new();
        let mut shuffled = Vec::new();
        for index in 0..(DEFINITION_PAGE_LIMIT * 3 + 17) {
            let mut definition = qdef();
            definition.queue_id = QueueId::new(format!("queue-{index:05}")).unwrap();
            expected.push(definition.clone());
            shuffled.push(definition);
        }
        // A deterministic non-key order models HashMap values without making the proof depend on a
        // particular randomized hasher seed.
        shuffled.reverse();
        shuffled.rotate_left(113);

        let mut cursor = None;
        let mut observed = Vec::new();
        loop {
            let page = definition_page_from_sorted_rows(
                shuffled.clone(),
                cursor.as_ref(),
                DEFINITION_PAGE_LIMIT,
                None,
            )
            .unwrap();
            observed.extend(page.definitions);
            let Some(next) = page.next else { break };
            cursor = Some(next);
        }
        assert_eq!(observed, expected);
    }

    fn ts(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(seconds, 0).unwrap()
    }

    #[test]
    fn in_process_async_control_plane_delegates_in_one_poll() {
        let control = InProcessControlPlane::new();
        let mut create = Box::pin(crate::AsyncControlPlane::create_queue(&control, qdef()));
        assert!(matches!(poll_once(&mut create), Poll::Ready(Ok(outcome)) if outcome.created));

        let mut get = Box::pin(crate::AsyncControlPlane::queue_definition(
            &control,
            queue(),
        ));
        assert!(matches!(poll_once(&mut get), Poll::Ready(Ok(definition)) if definition == qdef()));

        let mut list = Box::pin(crate::AsyncControlPlane::list_queues(
            &control,
            TenantId::new("tenant").unwrap(),
        ));
        assert!(
            matches!(poll_once(&mut list), Poll::Ready(Ok(queues)) if queues == vec![QueueId::new("queue").unwrap()])
        );
    }

    #[test]
    fn in_process_control_plane_concurrent_creates_are_create_or_read() {
        let control = Arc::new(InProcessControlPlane::new());
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let control = Arc::clone(&control);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    ControlPlane::create_queue(control.as_ref(), qdef())
                })
            })
            .collect::<Vec<_>>();

        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
        assert!(outcomes.iter().all(|outcome| outcome.definition == qdef()));
    }

    #[test]
    fn composed_backend_concurrent_compatible_creates_return_winning_definition() {
        let backend = Arc::new(ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        ));
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    futures::executor::block_on(backend.create_queue(qdef()))
                })
            })
            .collect::<Vec<_>>();

        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
        assert!(outcomes.iter().all(|outcome| outcome.definition == qdef()));
        assert_eq!(
            futures::executor::block_on(backend.queue_definition(&queue())).unwrap(),
            qdef()
        );
    }

    #[test]
    fn composed_backend_concurrent_incompatible_losers_conflict() {
        let backend = Arc::new(ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        ));
        futures::executor::block_on(backend.create_queue(qdef())).unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut definition = qdef();
                    definition.ordering_mode = OrderingMode::BoundedRelaxed;
                    barrier.wait();
                    futures::executor::block_on(backend.create_queue(definition))
                })
            })
            .collect::<Vec<_>>();

        let mut conflicts = 0;
        for result in handles.into_iter().map(|handle| handle.join().unwrap()) {
            match result {
                Err(EngineError::QueueDefinitionConflict) => conflicts += 1,
                other => panic!("unexpected create result: {other:?}"),
            }
        }
        assert_eq!(conflicts, 8);
    }

    fn add_millis(timestamp: UtcTimestamp, millis: u64) -> UtcTimestamp {
        let seconds = timestamp.seconds + (millis / 1_000) as i64;
        let nanos = timestamp.nanoseconds as u64 + (millis % 1_000) * 1_000_000;
        UtcTimestamp::new(
            seconds + (nanos / 1_000_000_000) as i64,
            (nanos % 1_000_000_000) as u32,
        )
        .unwrap()
    }

    fn pause_envelope(
        drain_intake: bool,
        seq: &str,
        item_id: ItemId,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(seq),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::PauseQueue(PauseQueueCommand { drain_intake }),
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        Pin::new(future).poll(&mut cx)
    }

    #[test]
    fn raw_backend_write_consumes_projection_admission_before_append() {
        let backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let shard = queue();
        assert!(matches!(
            poll_once(&mut backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));
        backend.with_projection(|projection| {
            projection
                .state
                .lock()
                .expect("fake projection poisoned")
                .reject_admission = true;
        });
        let envelope = pause_envelope(false, "raw-admission", ItemId::new("1").unwrap(), ts(0));
        let mut write = backend.commit_raw(crate::RawCommitRequest::new(shard, vec![envelope], 0));
        assert!(matches!(
            poll_once(&mut write),
            Poll::Ready(Err(EngineError::Unavailable))
        ));
        assert_eq!(backend.with_log(FakeGroupCommitLog::entry_count), 0);
    }

    #[test]
    fn lifecycle_quiescence_seals_accepted_group_commit_writes_before_reset_work() {
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

        let mut push = backend.push(&shard, vec![PushSpec::default()], ts(0), Some(epoch));
        assert!(matches!(poll_once(&mut push), Poll::Pending));
        assert_eq!(backend.buffered_group_commit_commands(), 1);

        backend
            .with_quiesced_log_and_projection_mut(|log, projection| {
                assert_eq!(log.sealed_batches(), vec![1]);
                assert_eq!(
                    projection
                        .state
                        .lock()
                        .expect("fake projection poisoned")
                        .pending
                        .len(),
                    1,
                    "live apply completes before lifecycle reset work starts"
                );
                Ok(())
            })
            .unwrap();

        assert_eq!(backend.buffered_group_commit_commands(), 0);
        assert!(matches!(poll_once(&mut push), Poll::Ready(Ok(_))));
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
            eligibility_time: None,
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
            eligibility_time: None,
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

    #[test]
    fn pause_blocks_claims_and_optionally_intake() {
        let backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let shard = queue();
        assert!(matches!(
            poll_once(&mut backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));

        let mut push_one = backend.push(&shard, vec![PushSpec::default()], ts(0), None);
        assert!(matches!(poll_once(&mut push_one), Poll::Ready(Ok(ids)) if ids.len() == 1));
        drop(push_one);

        let pause_env = pause_envelope(true, "pause-intake", ItemId::from_u64(1), ts(1));
        let mut pause_write = backend.commit_raw(crate::RawCommitRequest::new(
            shard.clone(),
            vec![pause_env],
            0,
        ));
        assert!(matches!(poll_once(&mut pause_write), Poll::Ready(Ok(_))));

        let mut paused_claim = backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-paused").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut paused_claim), Poll::Ready(Ok(claimed)) if claimed.items.is_empty()),
            "paused queue returns no claims"
        );

        let mut blocked_push = backend.push(&shard, vec![PushSpec::default()], ts(2), None);
        assert!(
            matches!(
                poll_once(&mut blocked_push),
                Poll::Ready(Err(EngineError::Paused { drain_intake: true }))
            ),
            "intake-blocking pause rejects pushes"
        );

        let mut resume_write = backend.commit_raw(crate::RawCommitRequest::new(
            shard.clone(),
            vec![CommandEnvelope {
                command_id: CommandId::new("resume-1"),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: vec![],
                command: QueueCommand::ResumeQueue,
                checksum: CommandChecksum(0),
                created_at: ts(3),
            }],
            0,
        ));
        assert!(matches!(poll_once(&mut resume_write), Poll::Ready(Ok(_))));

        let mut resumed_claim = backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer-2").unwrap(),
            max_items: 2,
            lease_token: LeaseToken::new("lease-resumed").unwrap(),
            lease_expires_at: ts(60),
            now: ts(4),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut resumed_claim), Poll::Ready(Ok(claimed)) if claimed.items.len() == 1)
        );

        let plain_backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let plain_shard = queue();
        assert!(matches!(
            poll_once(&mut plain_backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));
        let pause_env = pause_envelope(false, "pause-plain", ItemId::from_u64(1), ts(0));
        let mut pause_write = plain_backend.commit_raw(crate::RawCommitRequest::new(
            plain_shard.clone(),
            vec![pause_env],
            0,
        ));
        assert!(matches!(poll_once(&mut pause_write), Poll::Ready(Ok(_))));

        let mut landed_push =
            plain_backend.push(&plain_shard, vec![PushSpec::default()], ts(1), None);
        assert!(matches!(poll_once(&mut landed_push), Poll::Ready(Ok(ids)) if ids.len() == 1));
        drop(landed_push);

        let mut plain_claim = plain_backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: plain_shard.clone(),
            worker_id: WorkerId::new("claimer-3").unwrap(),
            max_items: 2,
            lease_token: LeaseToken::new("lease-plain").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut plain_claim), Poll::Ready(Ok(claimed)) if claimed.items.is_empty()),
            "plain pause still stops claims"
        );

        let resume_env = CommandEnvelope {
            command_id: CommandId::new("resume-plain"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![],
            command: QueueCommand::ResumeQueue,
            checksum: CommandChecksum(0),
            created_at: ts(2),
        };
        let mut resume_write = plain_backend.commit_raw(crate::RawCommitRequest::new(
            plain_shard.clone(),
            vec![resume_env],
            0,
        ));
        assert!(matches!(poll_once(&mut resume_write), Poll::Ready(Ok(_))));

        let mut resumed_plain_claim = plain_backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: plain_shard,
            worker_id: WorkerId::new("claimer-4").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-plain-2").unwrap(),
            lease_expires_at: ts(60),
            now: ts(2),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut resumed_plain_claim), Poll::Ready(Ok(claimed)) if claimed.items.len() == 1)
        );
    }

    #[test]
    fn pause_does_not_alter_lease_clock() {
        let backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let shard = queue();
        assert!(matches!(
            poll_once(&mut backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));

        let mut push_one = backend.push(&shard, vec![PushSpec::default()], ts(0), None);
        let first_id = match poll_once(&mut push_one) {
            Poll::Ready(Ok(ids)) => ids[0],
            other => panic!("unexpected push result: {other:?}"),
        };
        drop(push_one);

        let mut initial_claim = backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer-lease").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(10),
            now: ts(0),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut initial_claim), Poll::Ready(Ok(claimed)) if claimed.items[0].item_id == first_id)
        );

        let pause_env = pause_envelope(true, "pause-lease", ItemId::from_u64(1), ts(1));
        let mut pause_write = backend.commit_raw(crate::RawCommitRequest::new(
            shard.clone(),
            vec![pause_env],
            0,
        ));
        assert!(matches!(poll_once(&mut pause_write), Poll::Ready(Ok(_))));

        let mut reclaim = backend.reclaim_expired(&shard, None, ts(20), None);
        assert!(matches!(poll_once(&mut reclaim), Poll::Ready(Ok(ids)) if ids == vec![first_id]));
        drop(reclaim);

        let mut paused_claim = backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer-paused").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-paused").unwrap(),
            lease_expires_at: ts(30),
            now: ts(20),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut paused_claim), Poll::Ready(Ok(claimed)) if claimed.items.is_empty()),
            "paused queue still withholds claims"
        );

        let resume_env = CommandEnvelope {
            command_id: CommandId::new("resume-lease"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![],
            command: QueueCommand::ResumeQueue,
            checksum: CommandChecksum(0),
            created_at: ts(21),
        };
        let mut resume_write = backend.commit_raw(crate::RawCommitRequest::new(
            shard.clone(),
            vec![resume_env],
            0,
        ));
        assert!(matches!(poll_once(&mut resume_write), Poll::Ready(Ok(_))));

        let mut resumed_claim = backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("claimer-resumed").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-resumed").unwrap(),
            lease_expires_at: ts(40),
            now: ts(20),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        });
        assert!(
            matches!(poll_once(&mut resumed_claim), Poll::Ready(Ok(claimed)) if claimed.items[0].item_id == first_id)
        );
    }

    type SeenKey = (TenantId, fireweed_core::QueueId, Option<ItemId>, u64, u64);

    fn push_env(seq: &str, item_id: u64, created_at: UtcTimestamp) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(seq),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![ItemId::from_u64(item_id)],
            command: QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: fireweed_core::ClientItemKey::new(format!("k-{item_id}"))
                        .unwrap(),
                    item_id: ItemId::from_u64(item_id),
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
                }],
            }),
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        state: Mutex<RecordingSinkState>,
    }

    #[derive(Default)]
    struct RecordingSinkState {
        batches: Vec<Vec<SeenKey>>,
        seen: BTreeSet<SeenKey>,
        fail_next_emit: bool,
    }

    impl RecordingSink {
        fn batches(&self) -> Vec<Vec<SeenKey>> {
            self.state.lock().expect("sink poisoned").batches.clone()
        }

        fn seen(&self) -> BTreeSet<SeenKey> {
            self.state.lock().expect("sink poisoned").seen.clone()
        }

        fn fail_next_emit(&self) {
            self.state.lock().expect("sink poisoned").fail_next_emit = true;
        }
    }

    impl crate::port::ChangeRecordSink for RecordingSink {
        fn emit(
            &self,
            _shard: &QueueKey,
            records: &[crate::command::ChangeRecord],
        ) -> EngineResult<()> {
            let mut state = self.state.lock().expect("sink poisoned");
            let batch = records
                .iter()
                .map(crate::command::ChangeRecord::idempotency_key)
                .collect::<Vec<_>>();
            for key in &batch {
                state.seen.insert(key.clone());
            }
            state.batches.push(batch);
            if state.fail_next_emit {
                state.fail_next_emit = false;
                return Err(EngineError::Unavailable);
            }
            Ok(())
        }
    }

    fn seed_tail(log: &FakeGroupCommitLog, shard: &QueueKey, epoch: u64, count: u64) {
        let entries = (0..count)
            .map(|i| push_env(&format!("cmd-{i}"), i + 1, ts(10 + i as i64)))
            .collect::<Vec<_>>();
        let _ = log.set_entries(shard, epoch, entries);
    }

    fn restart_backend(
        log: &FakeGroupCommitLog,
    ) -> ComposedBackend<FakeGroupCommitLog, FakeProjection, InProcessControlPlane> {
        let backend = ComposedBackend::new(
            log.clone(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let recovered_shards = log
            .state
            .lock()
            .expect("fake log poisoned")
            .entries
            .iter()
            .map(|(position, _)| position.queue.clone())
            .collect::<HashSet<_>>();
        backend
            .inner
            .lock()
            .expect("composed backend poisoned")
            .known_shards
            .extend(recovered_shards);
        backend
    }

    #[test]
    fn change_record_cursor_advances_only_after_successful_emit() {
        let log = FakeGroupCommitLog::default();
        let shard = queue();
        seed_tail(&log, &shard, 7, 1);
        let backend = restart_backend(&log);
        let sink = RecordingSink::default();
        sink.fail_next_emit();

        assert!(matches!(
            backend.emit_change_record_tail(&shard, &sink, 1, ts(123), None),
            Err(EngineError::Unavailable)
        ));
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            None
        );
        assert_eq!(sink.batches().len(), 1);
        assert_eq!(
            sink.batches()[0],
            vec![(
                shard.tenant_id.clone(),
                shard.queue_id.clone(),
                Some(ItemId::from_u64(1)),
                7,
                0
            )]
        );

        assert_eq!(
            backend
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .unwrap(),
            1
        );
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
            Some(CommandPosition::new(shard.clone(), 7, 0))
        );
    }

    #[test]
    fn change_record_tail_is_emitted_in_command_position_order() {
        let log = FakeGroupCommitLog::default();
        let shard = queue();
        seed_tail(&log, &shard, 7, 3);
        let backend = restart_backend(&log);
        let sink = RecordingSink::default();

        assert_eq!(
            backend
                .emit_change_record_tail(&shard, &sink, 3, ts(123), None)
                .unwrap(),
            3
        );

        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(
            batches[0],
            vec![
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(1)),
                    7,
                    0
                ),
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(2)),
                    7,
                    1
                ),
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(3)),
                    7,
                    2
                ),
            ]
        );
        assert_eq!(
            sink.seen(),
            BTreeSet::from([
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(1)),
                    7,
                    0,
                ),
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(2)),
                    7,
                    1,
                ),
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(3)),
                    7,
                    2,
                ),
            ])
        );
    }

    #[test]
    fn emission_cursor_recovers_from_crash_without_skipping() {
        let log = FakeGroupCommitLog::default();
        let shard = queue();
        seed_tail(&log, &shard, 7, 3);
        log.fail_next_cursor_write();
        let backend = restart_backend(&log);
        let sink = RecordingSink::default();

        assert!(
            backend
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .is_err()
        );
        let restarted = restart_backend(&backend.with_log(|log| log.clone()));
        assert_eq!(
            restarted.with_log(|log| log.current_epoch(&shard).unwrap()),
            7
        );

        assert_eq!(
            restarted
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .unwrap(),
            1
        );
        assert_eq!(
            restarted
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .unwrap(),
            1
        );
        assert_eq!(
            restarted
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .unwrap(),
            1
        );

        let batches = sink.batches();
        assert_eq!(batches.len(), 4);
        assert_eq!(
            batches[0],
            vec![(
                shard.tenant_id.clone(),
                shard.queue_id.clone(),
                Some(ItemId::from_u64(1)),
                7,
                0
            )]
        );
        assert_eq!(batches[1], batches[0]);
        assert_eq!(
            batches[2],
            vec![(
                shard.tenant_id.clone(),
                shard.queue_id.clone(),
                Some(ItemId::from_u64(2)),
                7,
                1
            )]
        );
        assert_eq!(
            batches[3],
            vec![(
                shard.tenant_id.clone(),
                shard.queue_id.clone(),
                Some(ItemId::from_u64(3)),
                7,
                2
            )]
        );
        assert_eq!(
            sink.seen(),
            BTreeSet::from([
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(1)),
                    7,
                    0,
                ),
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(2)),
                    7,
                    1,
                ),
                (
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(3)),
                    7,
                    2,
                ),
            ])
        );
    }

    #[test]
    fn emission_cursor_failover_keeps_stable_dedup_key() {
        let log = FakeGroupCommitLog::default();
        let shard = queue();
        seed_tail(&log, &shard, 7, 1);
        log.fail_next_cursor_write();
        let backend = restart_backend(&log);
        let sink = RecordingSink::default();

        assert!(
            backend
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .is_err()
        );

        let restarted_log = backend.with_log(|log| log.clone());
        {
            let mut state = restarted_log.state.lock().expect("fake log poisoned");
            state.epoch = 8;
        }
        let restarted = restart_backend(&restarted_log);
        assert_eq!(
            restarted.with_log(|log| log.current_epoch(&shard).unwrap()),
            8
        );

        assert_eq!(
            restarted
                .emit_change_record_tail(&shard, &sink, 1, ts(123), None)
                .unwrap(),
            1
        );
        assert_eq!(
            sink.seen(),
            BTreeSet::from([(
                shard.tenant_id.clone(),
                shard.queue_id.clone(),
                Some(ItemId::from_u64(1)),
                7,
                0,
            )])
        );
        assert_eq!(
            sink.batches(),
            vec![
                vec![(
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(1)),
                    7,
                    0,
                )],
                vec![(
                    shard.tenant_id.clone(),
                    shard.queue_id.clone(),
                    Some(ItemId::from_u64(1)),
                    7,
                    0,
                )],
            ]
        );
    }

    #[test]
    fn composed_terminal_reap_on_reclaim_loop() {
        let backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let shard = queue();
        assert!(matches!(
            poll_once(&mut backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));

        let item_id = ItemId::from_u64(99);
        let terminal_position = CommandPosition::new(shard.clone(), 0, 2);
        backend
            .inner
            .lock()
            .expect("backend poisoned")
            .projection
            .seed_terminal_item(item_id, ts(0), Some(terminal_position.clone()));
        {
            let mut g = backend.inner.lock().expect("backend poisoned");
            g.log
                .set_emission_cursor(&shard, terminal_position.clone())
                .expect("seed emission cursor");
        }

        assert!(matches!(
            poll_once(&mut backend.tick(ts(61))),
            Poll::Ready(Ok(_))
        ));
        assert_eq!(
            poll_once(&mut backend.metrics(&shard)),
            Poll::Ready(Ok(QueueMetrics {
                pending: 0,
                resident_terminal_count: 0,
                ..Default::default()
            }))
        );
        assert!(
            backend
                .inner
                .lock()
                .expect("backend poisoned")
                .projection
                .state
                .lock()
                .expect("fake projection poisoned")
                .terminal
                .is_empty(),
            "scheduled reclaim should remove durable terminal rows once the cursor is safe"
        );
    }

    #[test]
    fn composed_terminal_reap_waits_for_durable_emission_cursor() {
        let backend = ComposedBackend::new(
            FakeGroupCommitLog::default(),
            FakeProjection::default(),
            InProcessControlPlane::new(),
        );
        let shard = queue();
        assert!(matches!(
            poll_once(&mut backend.create_queue(qdef())),
            Poll::Ready(Ok(_))
        ));

        let item_id = ItemId::from_u64(100);
        let terminal_position = CommandPosition::new(shard.clone(), 0, 2);
        backend
            .inner
            .lock()
            .expect("backend poisoned")
            .projection
            .seed_terminal_item(item_id, ts(0), Some(terminal_position.clone()));

        assert!(matches!(
            poll_once(&mut backend.tick(ts(61))),
            Poll::Ready(Ok(_))
        ));
        assert_eq!(
            poll_once(&mut backend.metrics(&shard)),
            Poll::Ready(Ok(QueueMetrics {
                pending: 0,
                resident_terminal_count: 1,
                ..Default::default()
            }))
        );
        assert!(
            backend
                .inner
                .lock()
                .expect("backend poisoned")
                .projection
                .state
                .lock()
                .expect("fake projection poisoned")
                .terminal
                .contains_key(&item_id),
            "emit-enabled queues must stay fail-closed while the durable cursor is missing"
        );

        {
            let mut g = backend.inner.lock().expect("backend poisoned");
            g.log
                .set_emission_cursor(&shard, terminal_position)
                .expect("seed emission cursor");
        }

        assert!(matches!(
            poll_once(&mut backend.tick(ts(61))),
            Poll::Ready(Ok(_))
        ));
        assert_eq!(
            poll_once(&mut backend.metrics(&shard)),
            Poll::Ready(Ok(QueueMetrics {
                pending: 0,
                resident_terminal_count: 0,
                ..Default::default()
            }))
        );
    }

    #[test]
    fn renewed_query_claim_rebuild_extends_replay_and_trim_pin_horizon() {
        let log = FakeGroupCommitLog::default();
        let shard = queue();
        let item_id = ItemId::from_u64(77);
        let lease_token = LeaseToken::new("query-lease").unwrap();
        let request_id = RequestId::new("query-request").unwrap();
        let fingerprint = 7_u64;
        log.set_entries(
            &shard,
            0,
            vec![
                CommandEnvelope {
                    command_id: CommandId::new("query-claim"),
                    request_id: Some(request_id.clone()),
                    request_fingerprint: Some(fingerprint),
                    request_outcome: Some(RequestOutcome::ClaimByQuery {
                        item_ids: vec![item_id],
                        lease_token: lease_token.clone(),
                        worker_id: Some(WorkerId::new("query-worker").unwrap()),
                    }),
                    item_ids: vec![item_id],
                    command: QueueCommand::Claim(ClaimCommand {
                        item_ids: vec![item_id],
                        lease_token: lease_token.clone(),
                        lease_expires_at: ts(130),
                        worker_id: Some(WorkerId::new("query-worker").unwrap()),
                    }),
                    checksum: CommandChecksum(0),
                    created_at: ts(100),
                },
                CommandEnvelope {
                    command_id: CommandId::new("renew-query-claim"),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids: vec![item_id],
                    command: QueueCommand::RenewLease(RenewLeaseCommand {
                        item_ids: vec![item_id],
                        lease_expires_at: ts(160),
                    }),
                    checksum: CommandChecksum(0),
                    created_at: ts(105),
                },
            ],
        );
        let mut recovered = HashMap::new();
        let mut recovered_push = HashMap::new();
        let mut recovered_commit = HashMap::new();
        ComposedBackend::<FakeGroupCommitLog, FakeProjection, InProcessControlPlane>::rebuild_idempotency_from_log(
            &log,
            RecoveryIdempotencyCaches {
                push: &mut recovered_push,
                claim: &mut recovered,
                commit: &mut recovered_commit,
            },
            &shard,
            10_000,
            None,
            &QueueCounters::default(),
        )
        .unwrap();
        assert_eq!(
            log.read_calls(),
            1,
            "all retained idempotency folds share one page read"
        );
        let cache = recovered.get(&shard).unwrap();
        assert!(matches!(
            cache.check_conflict_first(&request_id, BodyHash(fingerprint), ts(145)),
            IdempotencyDecision::Replay(_)
        ));
        assert!(cache.has_unexpired_matching(ts(145), |(item_ids, _)| !item_ids.is_empty()));
        assert!(matches!(
            cache.check_conflict_first(&request_id, BodyHash(fingerprint), ts(160)),
            IdempotencyDecision::Expired
        ));
    }

    #[test]
    fn retained_history_recovery_reads_each_page_once_for_all_idempotency_families() {
        let log = FakeGroupCommitLog::default();
        let shard = queue();
        let entries = (0..(RECOVERY_READ_PAGE_LIMIT + 1))
            .map(|sequence| CommandEnvelope {
                command_id: CommandId::new(format!("cmp-0-{sequence}")),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: Vec::new(),
                command: QueueCommand::CreateQueue(crate::command::CreateQueueCommand {
                    definition: qdef(),
                }),
                checksum: CommandChecksum(0),
                created_at: ts(0),
            })
            .collect();
        log.set_entries(&shard, 0, entries);

        let mut recovered_push = HashMap::new();
        let mut recovered_claim = HashMap::new();
        let mut recovered_commit = HashMap::new();
        ComposedBackend::<FakeGroupCommitLog, FakeProjection, InProcessControlPlane>::rebuild_idempotency_from_log(
            &log,
            RecoveryIdempotencyCaches {
                push: &mut recovered_push,
                claim: &mut recovered_claim,
                commit: &mut recovered_commit,
            },
            &shard,
            60_000,
            None,
            &QueueCounters::default(),
        )
        .unwrap();
        assert_eq!(
            log.read_calls(),
            2,
            "8,193 retained commands require two pages, not six family-specific reads"
        );
    }

    #[test]
    fn create_loser_later_page_failure_retries_without_partial_projection_or_publication() {
        let shard = queue();
        let definition = qdef();
        let log = FakeGroupCommitLog::default();
        log.page_reads_at_most(1);
        log.set_entries(
            &shard,
            0,
            vec![push_env("cmp-0-0", 1, ts(1)), push_env("cmp-0-1", 2, ts(2))],
        );
        log.fail_read_call_once(2);
        let control = InProcessControlPlane::new();
        assert!(control.create_queue(definition.clone()).unwrap().created);
        let backend = ComposedBackend::new(log, FakeProjection::default(), control);

        assert!(matches!(
            futures::executor::block_on(backend.create_queue(definition.clone())),
            Err(EngineError::Storage(message))
                if message.contains("later replay page read failed")
        ));

        let rejected_snapshot_position = CommandPosition::new(shard.clone(), 0, 1);
        let failed_epoch = futures::executor::block_on(backend.acquire_epoch(&shard));
        assert!(
            matches!(failed_epoch, Err(EngineError::NotFound)),
            "failed create must not let the unpublished handle advance the durable epoch: {failed_epoch:?}"
        );
        let failed_high_water = futures::executor::block_on(
            backend.set_high_water(&shard, rejected_snapshot_position.clone()),
        );
        assert!(
            matches!(failed_high_water, Err(EngineError::NotFound)),
            "failed create must not let the unpublished handle advance high-water: {failed_high_water:?}"
        );
        let failed_snapshot = futures::executor::block_on(backend.write_snapshot(
            &shard,
            rejected_snapshot_position,
            ProjectionSnapshot {
                payload: b"must-not-publish".to_vec(),
            },
        ));
        assert!(
            matches!(failed_snapshot, Err(EngineError::NotFound)),
            "failed create must not let the unpublished handle persist a snapshot: {failed_snapshot:?}"
        );
        let failed_emission_sink = RecordingSink::default();
        let failed_emission =
            backend.emit_change_record_tail(&shard, &failed_emission_sink, 1, ts(3), None);
        assert!(
            matches!(failed_emission, Err(EngineError::NotFound)),
            "failed create must not emit or advance a cursor for an unpublished shard: {failed_emission:?}"
        );
        assert!(
            failed_emission_sink.batches().is_empty(),
            "admission must precede external emission"
        );
        let failed_reap = backend.reap_terminal_items(&shard, ts(3), 0, false);
        assert!(
            matches!(failed_reap, Err(EngineError::NotFound)),
            "failed create must not reap an unpublished projection: {failed_reap:?}"
        );
        let failed_trim = backend.trim_reclaimable_segments(&shard, 0, ts(3));
        assert!(
            matches!(failed_trim, Err(EngineError::NotFound)),
            "failed create must not run log maintenance for an unpublished shard: {failed_trim:?}"
        );
        backend.with_log(|log| {
            let state = log.state.lock().expect("fake log poisoned");
            assert_eq!(
                state.epoch, 0,
                "rejected epoch acquisition changes no metadata"
            );
            assert_eq!(
                state.high_water, None,
                "rejected high-water update changes no metadata"
            );
            assert!(
                state.snapshots.is_empty(),
                "rejected snapshot write changes no metadata"
            );
            assert!(
                state.emission_cursor.is_empty(),
                "rejected emission changes no cursor metadata"
            );
        });

        let failed_push = futures::executor::block_on(backend.push(
            &shard,
            vec![PushSpec::default()],
            ts(3),
            None,
        ));
        assert!(
            matches!(failed_push, Err(EngineError::NotFound)),
            "failed create must reject push before reserving an id or command: {failed_push:?}"
        );
        backend.with_projection(|projection| {
            let state = projection.state.lock().expect("fake projection poisoned");
            assert!(
                state.pending.is_empty(),
                "failed read publishes no eligible items"
            );
            assert!(
                state.apply_batches.is_empty(),
                "failed read applies no replay prefix"
            );
        });
        backend.with_log(|log| {
            let state = log.state.lock().expect("fake log poisoned");
            assert_eq!(state.entries.len(), 2, "failed push appends no command");
        });

        let failed_claim = futures::executor::block_on(backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("failed-replay-worker").unwrap(),
            max_items: 2,
            lease_token: LeaseToken::new("failed-replay-lease").unwrap(),
            lease_expires_at: ts(30),
            now: ts(3),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        }));
        assert!(
            matches!(failed_claim, Err(EngineError::NotFound)),
            "failed create must not expose a data-plane shard: {failed_claim:?}"
        );

        let retry = futures::executor::block_on(backend.create_queue(definition))
            .expect("complete replay retries cleanly");
        assert!(!retry.created);
        backend.with_projection(|projection| {
            let state = projection.state.lock().expect("fake projection poisoned");
            assert_eq!(
                state.pending,
                vec![ItemId::from_u64(1), ItemId::from_u64(2)]
            );
            assert_eq!(state.apply_batches, vec![vec!["push", "push"]]);
        });
        assert_eq!(
            backend
                .with_projection(|projection| projection.metrics(&shard).unwrap())
                .pending,
            2,
            "retry exposes each replayed item and metric exactly once"
        );

        let claimed = futures::executor::block_on(backend.claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("replay-worker").unwrap(),
            max_items: 2,
            lease_token: LeaseToken::new("replay-lease").unwrap(),
            lease_expires_at: ts(30),
            now: ts(3),
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        }))
        .expect("replayed items remain lifecycle-valid");
        assert_eq!(claimed.items.len(), 2);

        let fresh_ids = futures::executor::block_on(backend.push(
            &shard,
            vec![PushSpec::default()],
            ts(4),
            None,
        ))
        .expect("counter and command sequence publish after successful replay");
        assert_eq!(
            fresh_ids,
            vec![ItemId::from_u64(3)],
            "failed push must consume neither the next item id nor a projection slot"
        );
        assert!(
            !claimed
                .items
                .iter()
                .any(|item| item.item_id == fresh_ids[0])
        );
        backend.with_log(|log| {
            let state = log.state.lock().expect("fake log poisoned");
            assert_eq!(state.entries.last().unwrap().1.command_id.0, "cmp-0-3");
        });
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
