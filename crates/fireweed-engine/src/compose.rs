//! # Orthogonal backend axes (ADR-012 residual)
//!
//! Product composition is **async-only**: [`crate::AsyncLogReplayBackend`] /
//! [`crate::assemble_async_log_replay`] (and async family factories). This module still defines the
//! sync axis traits ([`LogStore`], [`ProjectionStore`], [`ControlPlane`]) that
//! [`crate::InProcessLogStore`] / [`crate::InProcessProjectionStore`] bridge into async products,
//! plus a crate-private sync dual-stack orchestrator retained only for in-crate unit tests.
//!
//! ## The three axes
//!
//! - [`LogStore`] — the durable command log + the epoch/fence authority (co-located with the log,
//!   TD-003) + the replay cursor + snapshots + the `command_position` high-water.
//!   **Not a product composition surface** — see the trait docs.
//! - [`ProjectionStore`] — the materialized read model: the full read surface, the index queries, the
//!   pre-commit validation helpers, and the `apply` seam.
//! - [`ControlPlane`] — queue definitions + placement.
//!
//! ## Sync dual-stack residual
//!
//! `ComposedBackend` (crate-private) is the historical sync orchestrator. New product opens must not
//! use it. Prefer expanding native [`crate::AsyncLogStore`] impls so sync axis bridges can shrink.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use fireweed_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByQueryRequest, ClientItemKey,
    CohortId, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, GateKeyPolicy, GroupKey,
    GroupedAggregateRequest, GroupedAggregateResponse, ItemId, ItemState, LeaseToken, Metadata,
    MetricsByQueryRequest, OrderingMode, PriorityValue, QueryCapabilityFlags, QueueDefinition,
    QueueId, RangeScanRequest, RangeScanResponse, RequestId, TenantId, UtcTimestamp,
};

use crate::active_scope::{ActiveScope, DiscoveryGranularity};
use crate::async_composed::validate_push_shape;
use crate::claim_validation::{ClaimCompatibility, ClaimUnit, validate_claim_compatibility};
use crate::command::{
    AdvanceInstanceFenceCommand, ClaimCommand, CohortClaimCommand, CommandChecksum,
    CommandEnvelope, CommandId, CommitOutcomeEntry, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, MutateItemsCommand, PayloadUpdate, PurgeItemsCommand, PushCommand,
    PushItem, QueueCommand, QueueCounters, ReassignLeaseCommand, RenewLeaseCommand,
    ReplacePendingCommand, RequestOutcome, ScheduleUpdate, SetGatesCommand, UpdateFieldsCommand,
    WriteSideRecordsCommand, build_push_items, command_envelope_change_records,
    validate_gate_command, validate_gate_push, validate_request_replay_metadata,
};
use crate::error::{CommitRejection, EngineError, EngineResult};
use crate::finalize_validation::validate_purge_force;
use crate::idempotency::{IdempotencyDecision, QueueIdempotencyCache};
use crate::maintenance::{
    MaintenanceAuthoritySnapshot, MaintenanceCandidate, MaintenanceDisposition, MaintenanceFilter,
    MaintenanceObjectClass, MaintenancePolicy,
};
use crate::port::{
    AsOfProjectionStore, Backend, BatchUpdateItemRef, BatchUpdateOutcome, BatchUpdatePort,
    BatchUpdateRequest, BatchUpdateResponse, BatchUpdateValue, BoundedMutationContext, ClaimPort,
    ClaimRef, ClaimRequest, Claimed, ClaimedItem, CommandPage, CommitCapabilities,
    CommitEntryOutcome, CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionPort,
    ControlPlaneStore, CreateQueueOutcome, EntryRecovery, FinalizePort, HistoricalProjectionRead,
    IndexHit, IndexQueryPort, ItemMutationPort, ItemMutationRequest, ItemMutationResponse,
    ItemView, LeaseView, LiveItemView, LogRead, MaintenanceStopReason, MaintenanceSummary,
    PendingPage, PendingSummary, ProjectionRead, ProjectionSnapshot, PurgePort, PushPort, PushSpec,
    QueueMetrics, ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeasePort,
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
/// **Not a product composition surface.** Product backends assemble via
/// [`crate::AsyncLogReplayBackend`] / family factories. `LogStore` remains public because
/// [`crate::InProcessLogStore`] and [`crate::BlockingLogStore`] still bridge sync axis impls
/// (`MemoryLog`, `SqliteLog`, …) into [`crate::AsyncLogStore`] for
/// [`crate::assemble_async_log_replay`]. Prefer implementing `AsyncLogStore` natively when practical.
///
/// The composition holds the substrate under its unit-of-work lock and calls these methods with `&mut`
/// (writes) / `&` (reads) WHILE the lock is held, so append+apply is one atomic unit of work. Object
/// safety is not required — the composition is generic (zero-cost, monomorphized).
pub trait LogStore: Send {
    /// The durability class the composition inherits from its log axis (TD-007 §2). The default is
    /// [`DurabilityClass::Atomic`]; an object-log substrate whose projection may materialize after the
    /// authoritative append reports [`DurabilityClass::EventualApply`]. This class describes the visibility
    /// boundary, not whether the log can atomically commit a request-id-bearing command batch: both classes
    /// provide one durable append authority, and recovery replays that authority into the projection.
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    /// Whether this log substrate retains commands across process death (ADR-013 Class A).
    ///
    /// Default `true`. In-process logs (`fireweed_projection::MemoryLog`) return `false` (Class B):
    /// after process death only the projection remains, so recovery must not require a durable log
    /// tail or claim Class A rebuild-from-log semantics.
    fn is_durable_log(&self) -> bool {
        true
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

/// One version-fenced field update planned by a projection for an API-004 bounded mutation. The
/// composition durably appends these commands before applying them; projection adapters must not mutate
/// their serving image while producing the plan.
#[derive(Debug, Clone)]
pub struct BoundedMutationUpdate {
    pub command: UpdateFieldsCommand,
    pub expected_item_version: u64,
}

#[derive(Debug, Clone)]
pub struct BoundedMutationPlan {
    pub response: BoundedMutationResponse,
    pub updates: Vec<BoundedMutationUpdate>,
}

#[derive(Debug, Clone)]
pub struct ItemMutationPlan {
    pub response: ItemMutationResponse,
    pub command: MutateItemsCommand,
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

    /// Resolve a retained push request directly from a durable relational projection.
    ///
    /// Log-backed compositions rebuild their in-memory replay cache from command envelopes and inherit
    /// this no-op. A unified relational log/projection has no replayable command stream, so it overrides
    /// this seam to consult the request-outcome row written in the same transaction as the mutation.
    /// `Ok(Some(ids))` is an identical-body replay; a different retained body returns
    /// [`EngineError::RequestIdConflict`].
    fn replay_durable_push(
        &mut self,
        _shard: &QueueKey,
        _request_id: &RequestId,
        _items: &[PushSpec],
        _now: UtcTimestamp,
    ) -> EngineResult<Option<Vec<ItemId>>> {
        Ok(None)
    }

    /// Relational counterpart of [`Self::replay_durable_push`] for API-001 BatchUpdate.
    fn replay_durable_batch_update(
        &mut self,
        _shard: &QueueKey,
        _request_id: &RequestId,
        _fingerprint: u64,
        _now: UtcTimestamp,
    ) -> EngineResult<Option<BatchUpdateResponse>> {
        Ok(None)
    }

    /// Resolve a retained item-mutation response from unified relational authority without
    /// re-evaluating selectors against the current projection.
    fn replay_durable_item_mutation(
        &mut self,
        _shard: &QueueKey,
        _request_id: &RequestId,
        _fingerprint: u64,
        _now: UtcTimestamp,
    ) -> EngineResult<Option<ItemMutationResponse>> {
        Ok(None)
    }

    /// Resolve a retained vectorized commit from unified relational authority.
    fn replay_durable_commit(
        &mut self,
        _shard: &QueueKey,
        _request_id: &RequestId,
        _fingerprint: u64,
        _now: UtcTimestamp,
    ) -> EngineResult<Option<Vec<CommitOutcomeEntry>>> {
        Ok(None)
    }

    /// Read retained commit recovery without resubmitting the original request body.
    fn read_durable_commit(
        &self,
        _shard: &QueueKey,
        _request_id: &RequestId,
    ) -> EngineResult<Option<Vec<CommitOutcomeEntry>>> {
        Ok(None)
    }

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

    /// Point-lookup claim classification for API-001 `BatchClaimByItemIds`.
    ///
    /// Default is unavailable; memory/compose projections override with `O(1)` primary-key lookup.
    /// Implementations MUST NOT scan all eligible candidates of the shard per id.
    fn classify_claim_by_item_id(
        &self,
        _shard: &QueueKey,
        _id: &ItemId,
        _now: UtcTimestamp,
    ) -> EngineResult<fireweed_core::ClaimByItemIdClass> {
        Err(EngineError::Unavailable)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>>;

    /// Resolve every item/key referenced by one API-001 BatchUpdate against one projection snapshot.
    /// Implementations backed by relational storage override this with one set-oriented read. The
    /// returned rows may be in any order; orchestration restores request order from the request itself.
    fn batch_update_snapshot(
        &self,
        _shard: &QueueKey,
        _refs: &[BatchUpdateItemRef],
    ) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
        Err(EngineError::Unavailable)
    }

    /// Preflight the successful commands produced from the snapshot before the durable append. The bools
    /// align with `commands`; `false` is an entry-local validation rejection and does not reject siblings.
    fn batch_update_preflight(
        &self,
        _shard: &QueueKey,
        _commands: &[UpdateFieldsCommand],
    ) -> EngineResult<Vec<bool>> {
        Err(EngineError::Unavailable)
    }

    /// Resolve and validate one backend-erased mutation against a single immutable queue image. The
    /// returned command contains no selectors: only exact item ids and complete post-mutation values.
    fn plan_item_mutation(
        &self,
        _shard: &QueueKey,
        _request: &ItemMutationRequest,
    ) -> EngineResult<ItemMutationPlan> {
        Err(EngineError::Unavailable)
    }
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

    // -- rich claim/discovery and gate capabilities (BQ-14) ------------------------------------------
    //
    // Rich non-item claims remain relational-class. Gate storage and exact active-scope discovery are
    // supported by both the shared in-memory/log-replay projection and relational projections.

    /// Whether this projection stores gate membership + gate-state and enforces `SetGates` at claim
    /// selection. `false` (the default) makes the composition refuse gate-bearing pushes and `SetGates`;
    /// capable projections override it to `true`.
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

    /// Derive ranked [`ActiveScope`]s at `granularity` (BQ-14e). Implementations may read a relational
    /// summary or derive the exact live rollup directly from their materialized item projection.
    /// The default refuses with [`EngineError::Unavailable`].
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

    /// Plan, but do not apply, a bounded mutation. Log-backed compositions use this seam so each
    /// successful per-record update passes through the authoritative append/apply boundary.
    fn plan_bounded_mutation(
        &self,
        _shard: &QueueKey,
        _request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationPlan> {
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

/// The minimum immutable projection row needed to plan a BatchUpdate without scalar projection calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchUpdateSnapshotItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub state: ItemState,
    pub item_version: u64,
    pub fenced: bool,
    pub superseded: bool,
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
    batch_update: &'a mut HashMap<QueueKey, QueueIdempotencyCache<BatchUpdateResponse>>,
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

pub fn queue_worker_partition(queue: &QueueKey, partitions: usize) -> usize {
    assert!(
        partitions > 0,
        "queue worker partition count must be nonzero"
    );
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    queue.hash(&mut hasher);
    (hasher.finish() as usize) % partitions
}


// ---------------------------------------------------------------------------
// Sync dual-stack ComposedBackend deleted (program B Gates B). Product
// composition is AsyncLogReplayBackend / assemble_async_log_replay. This module
// retains LogStore / ProjectionStore / ControlPlane axes and pure helpers.
// ---------------------------------------------------------------------------

pub fn request_expires_at(now: UtcTimestamp, retention_ms: u64) -> UtcTimestamp {
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

/// Body fingerprint for API-004 ClaimByQuery (request_id excluded). Shared with async log-replay.
pub fn claim_by_query_body_hash(request: &ClaimByQueryRequest) -> EngineResult<BodyHash> {
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

/// Body fingerprint for API-001 BatchClaimByItemIds (`request_id` excluded).
pub fn claim_by_item_ids_body_hash(
    request: &fireweed_core::ClaimByItemIdsRequest,
) -> EngineResult<BodyHash> {
    use sha2::{Digest, Sha256};

    // request_id is the cache key; fingerprint covers the logical claim body only.
    let token = request
        .lease_token
        .as_ref()
        .map(|t| t.as_str())
        .unwrap_or("");
    let canonical = (
        &request.item_ids,
        request.lease_duration_ms,
        request.worker_id.as_str(),
        token,
    );
    let bytes = serde_json::to_vec(&canonical).map_err(|e| EngineError::Storage(e.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(BodyHash(u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    )))
}

/// Body fingerprint for API-001 BatchUpdate (request_id excluded). Shared with async log-replay.
pub fn batch_update_body_hash(request: &BatchUpdateRequest) -> EngineResult<BodyHash> {
    use sha2::{Digest, Sha256};

    // `request_id` is the cache key, not part of the logical request body.
    let bytes = serde_json::to_vec(&request.updates)
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(BodyHash(u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    )))
}

pub fn item_mutation_fingerprint(request: &ItemMutationRequest) -> EngineResult<u64> {
    use sha2::{Digest, Sha256};

    #[derive(serde::Serialize)]
    struct Body<'a> {
        evaluated_at: UtcTimestamp,
        dry_run: bool,
        returning: crate::port::ItemMutationReturning,
        gate_changes: &'a [crate::port::GateChange],
        operation: &'a crate::port::ItemMutationOperation,
    }
    let bytes = serde_json::to_vec(&Body {
        evaluated_at: request.evaluated_at,
        dry_run: request.dry_run,
        returning: request.returning,
        gate_changes: &request.gate_changes,
        operation: &request.operation,
    })
    .map_err(|error| EngineError::Storage(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    ))
}

fn item_mutation_body_hash(request: &ItemMutationRequest) -> EngineResult<BodyHash> {
    item_mutation_fingerprint(request).map(BodyHash)
}

/// Planner output for API-001 BatchUpdate (shared with async log-replay).
pub struct PlannedBatchUpdate {
    pub outcomes: Vec<BatchUpdateOutcome>,
    pub commands: Vec<(usize, UpdateFieldsCommand)>,
}

/// Plan per-entry BatchUpdate outcomes and UpdateFields commands (shared with async log-replay).
pub fn plan_batch_update(
    definition: &QueueDefinition,
    supports_gates: bool,
    updates: Vec<crate::port::BatchUpdateEntry>,
    snapshot: Vec<BatchUpdateSnapshotItem>,
) -> PlannedBatchUpdate {
    let mut by_id = HashMap::with_capacity(snapshot.len());
    let mut by_key = HashMap::<ClientItemKey, ItemId>::with_capacity(snapshot.len());
    for item in snapshot {
        // Superseded records retain their historical key in relational storage but are no longer the
        // active key mapping. Excluding them matches the in-memory projection's `by_key` semantics.
        if !item.superseded {
            by_key.insert(item.client_item_key.clone(), item.item_id);
        }
        by_id.insert(item.item_id, item);
    }

    let mut outcomes = vec![BatchUpdateOutcome::Conflict; updates.len()];
    let mut commands = Vec::with_capacity(updates.len());
    let mut seen = HashSet::with_capacity(updates.len());

    for (outcome_index, update) in updates.into_iter().enumerate() {
        let resolved = match &update.item_ref {
            BatchUpdateItemRef::ItemId(item_id) => Some(*item_id),
            BatchUpdateItemRef::ClientItemKey(key) => by_key.get(key).copied(),
            BatchUpdateItemRef::Both {
                item_id,
                client_item_key,
            } => match (by_id.get(item_id), by_key.get(client_item_key)) {
                (Some(_), Some(resolved)) if resolved == item_id => Some(*item_id),
                (Some(_), Some(_)) => {
                    outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                    continue;
                }
                _ => None,
            },
        };
        let Some(item_id) = resolved else {
            outcomes[outcome_index] = BatchUpdateOutcome::NotFound;
            continue;
        };
        let Some(current) = by_id.get(&item_id) else {
            outcomes[outcome_index] = BatchUpdateOutcome::NotFound;
            continue;
        };
        if !seen.insert(item_id) {
            outcomes[outcome_index] = BatchUpdateOutcome::Conflict;
            continue;
        }
        if current.state.is_terminal() {
            outcomes[outcome_index] = BatchUpdateOutcome::Terminal;
            continue;
        }
        if current.state != ItemState::Pending || current.superseded || current.fenced {
            outcomes[outcome_index] = BatchUpdateOutcome::Conflict;
            continue;
        }
        if update
            .expected_item_version
            .is_some_and(|expected| expected != current.item_version)
        {
            outcomes[outcome_index] = BatchUpdateOutcome::Conflict;
            continue;
        }

        let set_fields = match update.fields {
            BatchUpdateValue::Keep => None,
            BatchUpdateValue::Replace(fields) => {
                let reserved = fields.keys().cloned().map(|name| (name, None)).collect();
                if validate_api001_reserved_write_fields(&reserved).is_err() {
                    outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                    continue;
                }
                Some(fields)
            }
        };
        let set_metadata = match update.metadata {
            BatchUpdateValue::Keep => None,
            BatchUpdateValue::Replace(metadata) => Some(metadata),
        };
        let set_gate_keys = match update.gate_keys {
            BatchUpdateValue::Keep => None,
            BatchUpdateValue::Replace(mut gate_keys) => {
                gate_keys.sort();
                gate_keys.dedup();
                let malformed = gate_keys.iter().any(|key| {
                    key.is_empty()
                        || key.len() > 256
                        || !key
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
                });
                let disabled = definition.eligibility_policy.gate_keys
                    != fireweed_core::GateKeyPolicy::Dynamic
                    && !gate_keys.is_empty();
                let unsupported = !supports_gates && !gate_keys.is_empty();
                let over_cap = definition
                    .eligibility_policy
                    .max_gate_keys_per_item
                    .is_some_and(|max| gate_keys.len() as u64 > max);
                if malformed || disabled || unsupported || over_cap {
                    outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                    continue;
                }
                Some(gate_keys)
            }
        };
        let set_priority = match update.priority {
            BatchUpdateValue::Keep => ScheduleUpdate::Keep,
            BatchUpdateValue::Replace(priority) => {
                let type_matches = matches!(
                    (&definition.priority_model.kind, &priority),
                    (
                        fireweed_core::PriorityModelKind::Timestamp,
                        PriorityValue::Timestamp(_)
                    ) | (
                        fireweed_core::PriorityModelKind::Int64,
                        PriorityValue::Int64(_)
                    ) | (
                        fireweed_core::PriorityModelKind::Decimal,
                        PriorityValue::Decimal(_)
                    ) | (
                        fireweed_core::PriorityModelKind::Text,
                        PriorityValue::Text(_)
                    )
                );
                if !type_matches {
                    outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                    continue;
                }
                ScheduleUpdate::Set(Some(priority))
            }
        };
        let set_not_before = match update.not_before {
            BatchUpdateValue::Keep => ScheduleUpdate::Keep,
            BatchUpdateValue::Replace(value) => ScheduleUpdate::Set(value),
        };
        let payload = match update.payload {
            BatchUpdateValue::Keep => PayloadUpdate::Keep,
            BatchUpdateValue::Replace(value) => PayloadUpdate::Set(value),
        };

        outcomes[outcome_index] = BatchUpdateOutcome::Updated {
            item_id,
            client_item_key: current.client_item_key.clone(),
            item_version: current.item_version + 1,
        };
        commands.push((
            outcome_index,
            UpdateFieldsCommand {
                item_id,
                field_ops: BTreeMap::new(),
                payload,
                set_priority,
                set_not_before,
                set_entity_document: None,
                set_fields,
                set_metadata,
                set_gate_keys,
                api001_batch: true,
            },
        ));
    }

    PlannedBatchUpdate { outcomes, commands }
}

/// Stable body fingerprint for the vectorized commit path: a non-cryptographic hash over the serialized
/// commit entries (the request_id is the cache KEY, not part of the body). A different body under the same
/// request id is a `RequestIdConflict`; an equal body replays the prior per-entry outcomes.
/// Stable body fingerprint for the vectorized commit path (shared with async compositions).
pub fn commit_body_hash(entries: &[crate::port::CommitTransitionEntry]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

/// Project the retained per-entry recovery records into the public per-entry outcomes (the commit return /
/// replay value). The recovery record is the superset (it ALSO carries the consumed input id, instance
/// fence, and side-record keys for `explain_commit`).
/// Project retained recovery records into public per-entry outcomes (shared with async compositions).
pub fn outcomes_from_recovery(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
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
/// Project recovery into durable outcome entry (shared with async compositions).
pub fn outcome_entry_from_recovery(r: &EntryRecovery) -> CommitOutcomeEntry {
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
/// Reconstruct recovery from durable outcome entry (shared with async compositions).
pub fn recovery_from_outcome_entry(e: CommitOutcomeEntry) -> EntryRecovery {
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

fn valid_gate_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 256
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

/// Gate-key policy check against a queue definition (shared with async compositions).
pub fn validate_gate_key_sets<'a>(
    definition: &QueueDefinition,
    key_sets: impl IntoIterator<Item = &'a [String]>,
) -> EngineResult<()> {
    let key_sets = key_sets.into_iter().collect::<Vec<_>>();
    let has_keys = key_sets.iter().any(|keys| !keys.is_empty());
    if has_keys && definition.eligibility_policy.gate_keys != GateKeyPolicy::Dynamic {
        return Err(EngineError::Invalid("gates-not-enabled"));
    }
    let mut request_keys = BTreeSet::new();
    for keys in key_sets {
        if keys.iter().any(|key| !valid_gate_key(key)) {
            return Err(EngineError::Invalid("invalid gate key"));
        }
        if definition
            .eligibility_policy
            .max_gate_keys_per_item
            .is_some_and(|max| keys.len() as u64 > max)
        {
            return Err(EngineError::Invalid("item gate-key cap exceeded"));
        }
        request_keys.extend(keys.iter());
    }
    if definition
        .eligibility_policy
        .max_gates_per_request
        .is_some_and(|max| request_keys.len() as u64 > max)
    {
        return Err(EngineError::Invalid("request gate-key cap exceeded"));
    }
    Ok(())
}

/// Gate-command validation against queue definition (shared with async compositions).
pub fn validate_gate_command_definition(
    definition: &QueueDefinition,
    command: &QueueCommand,
) -> EngineResult<()> {
    match command {
        QueueCommand::SetGates(command) => {
            validate_gate_key_sets(definition, std::iter::once(command.gate_keys.as_slice()))
        }
        QueueCommand::Push(command) => validate_gate_key_sets(
            definition,
            command.items.iter().map(|item| item.gate_keys.as_slice()),
        ),
        QueueCommand::ReplacePending(command) => validate_gate_key_sets(
            definition,
            std::iter::once(command.replacement.gate_keys.as_slice()),
        ),
        _ => Ok(()),
    }
}
