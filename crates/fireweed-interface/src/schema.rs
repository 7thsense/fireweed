//! Space schema declarations (API-006 §Schema).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::expr::{Aggregate, Direction, Expr};
use crate::ids::{FieldName, JsonPointer, ObjectName, ParamName, ReadName};
use crate::value::ScalarType;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpaceSchema {
    /// Strictly increasing per space.
    pub version: u64,
    pub request_id_retention_ms: u64,
    pub queues: Vec<QueueDecl>,
    pub tables: Vec<TableDecl>,
    pub reads: Vec<ReadDecl>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldDecl {
    pub name: FieldName,
    pub ty: ScalarType,
    pub nullable: bool,
    pub source: FieldSource,
    pub index: IndexHint,
    /// How a write may change the field on an existing row (tables only).
    #[serde(default)]
    pub merge: MergeRule,
}

impl FieldDecl {
    pub fn stored(name: &str, ty: ScalarType, nullable: bool, index: IndexHint) -> Self {
        Self {
            name: FieldName::new(name),
            ty,
            nullable,
            source: FieldSource::Stored,
            index,
            merge: MergeRule::Any,
        }
    }

    pub fn with_merge(mut self, merge: MergeRule) -> Self {
        self.merge = merge;
        self
    }
}

/// A declared merge rule on a stored, non-key table field. A write that
/// breaks it rejects with `MergeRuleViolated`, unless it is a `Mutate` with
/// `override_merge_rules` (an explicit lift).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeRule {
    /// Any change.
    #[default]
    Any,
    /// Sets (`TextSet`): the new value contains the old one.
    AddOnly,
    /// `Int`, `Float`, `Timestamp`: the new value is not less than the old.
    NonDecreasing,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FieldSource {
    Stored,
    /// Derived from the JSON body; read-only.
    BodyPointer(JsonPointer),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexHint {
    None,
    Lookup,
    Range,
    Unique,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BodyKind {
    Bytes,
    Json,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueueDecl {
    pub name: ObjectName,
    pub spec: QueueSpec,
    pub body: BodyKind,
    pub fields: Vec<FieldDecl>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PriorityKind {
    Int64,
    Timestamp,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OrderingMode {
    Strict,
    BestEffort,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Backoff {
    Fixed { ms: u64 },
    Exponential { base_ms: u64, max_ms: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatePolicy {
    pub max_gate_keys_per_item: u32,
    pub max_gates_per_request: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortPolicy {
    pub completion_bound_ms: u64,
}

/// Interface-owned queue definition (replaces `fireweed-core::QueueDefinition`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueueSpec {
    pub priority_kind: PriorityKind,
    /// API-001 `priority_model.direction`.
    pub priority_direction: Direction,
    pub ordering: OrderingMode,
    pub max_attempts: u32,
    pub retry_backoff: Backoff,
    pub default_lease_duration_ms: u64,
    pub progress_bound_ms: Option<u64>,
    pub client_item_key_retention_ms: u64,
    pub terminal_retention_ms: u64,
    pub gates: Option<GatePolicy>,
    /// API-001 `eligibility_policy.metadata_blockers`.
    pub metadata_blockers: BTreeMap<String, Vec<serde_json::Value>>,
    pub cohorts: Option<CohortPolicy>,
    pub group_batching: bool,
    pub recurrence: bool,
}

impl QueueSpec {
    /// A strict, ascending queue with no optional features.
    pub fn basic(priority_kind: PriorityKind) -> Self {
        Self {
            priority_kind,
            priority_direction: Direction::Asc,
            ordering: OrderingMode::Strict,
            max_attempts: 3,
            retry_backoff: Backoff::Fixed { ms: 1000 },
            default_lease_duration_ms: 30_000,
            progress_bound_ms: None,
            client_item_key_retention_ms: 86_400_000,
            terminal_retention_ms: 86_400_000,
            gates: None,
            metadata_blockers: BTreeMap::new(),
            cohorts: None,
            group_batching: false,
            recurrence: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableDecl {
    pub name: ObjectName,
    /// 1 to 4 fields.
    pub key: Vec<FieldName>,
    /// Includes the key fields.
    pub fields: Vec<FieldDecl>,
    pub expiry: Option<RowExpiry>,
    /// Who chooses the key. `Sequence` makes an append-only table.
    #[serde(default)]
    pub key_assignment: KeyAssignment,
}

/// Who chooses a table's key.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyAssignment {
    /// Writers supply every key field.
    #[default]
    Caller,
    /// The key is one non-nullable `Int` field that the implementation
    /// assigns: 1, 2, 3, ... per table, gapless in commit order, never
    /// reused. Writers omit it. Rows are immutable (no `Set`); `Retire` and
    /// row expiry remove them. Readers page with a declared read ranged on
    /// the key.
    Sequence,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RowExpiry {
    AtField(FieldName),
    AfterWrite { ttl_ms: u64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadDecl {
    pub name: ReadName,
    pub version: u32,
    /// A queue or table.
    pub source: ObjectName,
    pub params: Vec<(ParamName, ScalarType)>,
    /// The index rule applies.
    pub filter: Expr,
    pub output: ReadOutput,
    /// Rows only; key order breaks ties.
    pub order: Vec<(FieldName, Direction)>,
    pub max_rows: u32,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ReadOutput {
    Rows {
        fields: Vec<FieldName>,
    },
    Aggregate {
        group_by: Vec<(FieldName, Expr)>,
        values: Vec<(FieldName, Aggregate)>,
    },
}

/// The stored schema and index readiness.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaState {
    pub schema: SpaceSchema,
    pub indexes: Vec<IndexState>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexState {
    pub object: ObjectName,
    pub field: FieldName,
    pub ready: bool,
}
