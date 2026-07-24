//! Engine-owned identity, position, and durability-class types.
//!
//! These are domain types (the command/projection model) — they live in the engine,
//! not in any storage adapter. Adapters depend on these; the engine depends on nothing
//! outward (ADR-007).

use fireweed_core::{QueueId, TenantId};

/// Tenant + queue identity — the unit a log/projection is owned and partitioned by.
///
/// The queue is the unit of sharding (ADR-008): a whole queue is owned by exactly one node, so the
/// log, projection, and ownership lease are all keyed by `(tenant_id, queue_id)`. A relational backend
/// MAY internally hash-partition its item table (`hash(tenant,queue) % N`, TD-002) for vacuum/index-size
/// isolation, but that partition is client-invisible and never an ownership/routing key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct QueueKey {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
}

impl QueueKey {
    pub fn new(tenant_id: TenantId, queue_id: QueueId) -> Self {
        Self {
            tenant_id,
            queue_id,
        }
    }
}

impl PartialOrd for QueueKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tenant_id
            .as_str()
            .cmp(other.tenant_id.as_str())
            .then(self.queue_id.as_str().cmp(other.queue_id.as_str()))
    }
}

/// Position of a committed command within a queue's log. Ordered by `(backend_epoch, sequence)`.
///
/// The engine derives `item_version` and the monotonic `command_position` high-water mark from
/// committed positions; per TD-007 §4 the high-water mark is persisted in the projection/snapshot
/// (not recomputed from a compacted log), so replay is monotonic under retention.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CommandPosition {
    pub queue: QueueKey,
    pub backend_epoch: u64,
    pub sequence: u64,
}

impl CommandPosition {
    pub fn new(queue: QueueKey, backend_epoch: u64, sequence: u64) -> Self {
        Self {
            queue,
            backend_epoch,
            sequence,
        }
    }

    /// Monotonic ordering within a queue: epoch first, then sequence. Positions on different queues
    /// are not comparable for monotonicity; `command_position` is per-queue (ADR-008).
    pub fn precedes(&self, other: &Self) -> bool {
        debug_assert_eq!(self.queue, other.queue, "positions on different queues");
        (self.backend_epoch, self.sequence) < (other.backend_epoch, other.sequence)
    }
}

/// Which consistency guarantees the engine may assume from a backend (TD-007 §1).
///
/// The engine relies only on the weakest guarantee a backend declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityClass {
    /// Append + apply commit together; post-commit projection is globally consistent.
    /// Invariant 1 & 2 hold strictly. Backends: memory, sqlite, postgres.
    Atomic,
    /// Log commit acks; projection applies within a bounded window. Self-read-after-write only.
    /// Priority order is "over applied state, eventual"; upsert is unavailable. Backend: objectlog.
    EventualApply,
}

impl DurabilityClass {
    /// Whether `UpsertPort::replace_if_pending` may be offered (TD-007 §2.3): atomic only.
    pub fn supports_upsert(self) -> bool {
        matches!(self, DurabilityClass::Atomic)
    }
}
