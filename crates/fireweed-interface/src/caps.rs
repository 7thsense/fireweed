//! Guarantees, capabilities and limits (API-006 §Guarantees and capabilities).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::schema::{OrderingMode, PriorityKind};

/// Niflheim's write modes (Niflheim ADR-002), used as levels a commit
/// reaches. Ordered: `FireAndForget < Tracked < Durable`.
///
/// - `FireAndForget`: visible; may be lost if the process or machine fails.
/// - `Tracked`: survives a process crash and a reopen of the
///   implementation's own storage (fsynced locally).
/// - `Durable`: also survives loss of that storage (in S3, or confirmed by a
///   synchronous standby).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DurabilityLevel {
    FireAndForget,
    Tracked,
    Durable,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Visibility {
    Immediate,
    Bounded { max_ms: u64 },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Sharing {
    SingleProcess,
    MultiProcess,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Guarantees {
    /// Minimum for every acknowledged commit.
    pub durability: DurabilityLevel,
    pub new_work_visibility: Visibility,
    pub sharing: Sharing,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ShapeKind {
    Queue,
    Table,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QueueFeature {
    DynamicGates,
    MetadataBlockers,
    Recurrence,
    GroupBatching,
    Cohorts,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    pub max_durability: DurabilityLevel,
    /// Queue and Table always.
    pub shapes: BTreeSet<ShapeKind>,
    pub queue_features: BTreeSet<QueueFeature>,
    pub priority_kinds: BTreeSet<PriorityKind>,
    pub ordering_modes: BTreeSet<OrderingMode>,
    pub limits: ImplementationLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImplementationLimits {
    pub max_ops_per_commit: u32,
    pub max_entries_per_commit: u32,
    pub max_request_bytes: u64,
    pub max_in_list: u32,
    pub max_writer_scan_rows: u64,
    pub max_read_scan_rows: u64,
    pub max_cohort_size: u32,
    pub max_selector_affected: u32,
    pub max_body_bytes: u64,
    pub max_value_bytes: u64,
    pub max_set_values: u32,
    pub max_dry_run_count: u64,
    pub max_read_rows: u32,
    pub max_claim_batch_size: u32,
    pub max_lease_duration_ms: u64,
    pub max_request_id_retention_ms: u64,
    pub max_objects_per_space: u32,
}
