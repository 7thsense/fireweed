//! Generic administrative proposals. Backend integration owns the containing
//! atomic group, selection fence, uniqueness and durable execution receipt.
use crate::queue_contract::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_SELECTOR_DEPTH: usize = 64;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Comparison {
    Equal,
    Before,
    BeforeOrEqual,
    After,
    AfterOrEqual,
}
impl Comparison {
    pub fn matches<T: Ord>(&self, actual: T, expected: T) -> bool {
        use std::cmp::Ordering::*;
        matches!(
            (self, actual.cmp(&expected)),
            (Self::Equal, Equal)
                | (Self::Before, Less)
                | (Self::BeforeOrEqual, Less | Equal)
                | (Self::After, Greater)
                | (Self::AfterOrEqual, Greater | Equal)
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkSelector {
    All(Vec<WorkSelector>),
    Any(Vec<WorkSelector>),
    Not(Box<WorkSelector>),
    Field { name: String, predicate: Predicate },
    Present { name: String, present: bool },
    Keys(BTreeSet<String>),
    StoredLifecycleIn(Vec<Lifecycle>),
    EffectiveLifecycleIn(Vec<Lifecycle>),
    Attempts { comparison: Comparison, value: u32 },
    AvailableAt { comparison: Comparison, value: i64 },
    LeaseActive(bool),
}
impl WorkSelector {
    /// Iterative depth validation must precede recursive wire serialization.
    pub fn validate(&self, definition: &CollectionDefinition) -> QueueResult<()> {
        let mut pending = vec![(std::slice::from_ref(self).iter(), 1usize)];
        while let Some((nodes, depth)) = pending.last_mut() {
            let depth = *depth;
            let Some(node) = nodes.next() else {
                pending.pop();
                continue;
            };
            if depth > MAX_SELECTOR_DEPTH {
                return Err(invalid("selector nesting exceeds common limit"));
            }
            match node {
                Self::All(nodes) | Self::Any(nodes) => pending.push((nodes.iter(), depth + 1)),
                Self::Not(node) => {
                    pending.push((std::slice::from_ref(node.as_ref()).iter(), depth + 1))
                }
                Self::Field { name, predicate } => {
                    validate_field_predicate(definition, name, predicate)?
                }
                Self::Present { name: field, .. } => {
                    name(field)?;
                    if !definition.admits_field(field) {
                        return Err(invalid("undeclared selector field"));
                    }
                }
                Self::Keys(keys) => {
                    for key in keys {
                        name(key)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// Reference oracle only: physical stores select natively, never hydrate a
    /// whole queue or claim broadly to evaluate this on the client.
    pub fn matches(&self, record: &WorkRecord, evaluated_at: i64) -> bool {
        enum Frame<'a> {
            Node(&'a WorkSelector),
            Not,
            Fold {
                nodes: std::slice::Iter<'a, WorkSelector>,
                all: bool,
                value: bool,
            },
            After {
                nodes: std::slice::Iter<'a, WorkSelector>,
                all: bool,
                value: bool,
            },
        }
        let mut frames = vec![Frame::Node(self)];
        let mut last = false;
        while let Some(frame) = frames.pop() {
            match frame {
                Frame::Not => last = !last,
                Frame::Fold {
                    mut nodes,
                    all,
                    value,
                } => {
                    if let Some(node) = nodes.next() {
                        frames.push(Frame::After { nodes, all, value });
                        frames.push(Frame::Node(node));
                    } else {
                        last = value;
                    }
                }
                Frame::After { nodes, all, value } => {
                    let value = if all { value && last } else { value || last };
                    if value != all {
                        last = value;
                    } else {
                        frames.push(Frame::Fold { nodes, all, value });
                    }
                }
                Frame::Node(node) => {
                    last = match node {
                        Self::All(nodes) | Self::Any(nodes) => {
                            let all = matches!(node, Self::All(_));
                            frames.push(Frame::Fold {
                                nodes: nodes.iter(),
                                all,
                                value: all,
                            });
                            continue;
                        }
                        Self::Not(node) => {
                            frames.push(Frame::Not);
                            frames.push(Frame::Node(node));
                            continue;
                        }
                        Self::Field { name, predicate } => {
                            predicate.matches(record.work.fields.get(name))
                        }
                        Self::Present { name, present } => {
                            record.work.fields.contains_key(name) == *present
                        }
                        Self::Keys(keys) => keys.contains(&record.work.key),
                        Self::StoredLifecycleIn(states) => states.contains(&record.lifecycle),
                        Self::EffectiveLifecycleIn(states) => {
                            states.contains(&effective_lifecycle(record, evaluated_at))
                        }
                        Self::Attempts { comparison, value } => {
                            comparison.matches(record.attempts, *value)
                        }
                        Self::AvailableAt { comparison, value } => {
                            comparison.matches(record.work.available_at, *value)
                        }
                        Self::LeaseActive(expected) => {
                            lease_active(record, evaluated_at) == *expected
                        }
                    };
                }
            }
        }
        last
    }
}

pub fn lease_active(record: &WorkRecord, evaluated_at: i64) -> bool {
    record.lifecycle == Lifecycle::Leased
        && record
            .lease_expires_at
            .is_some_and(|expiry| expiry > evaluated_at)
}
pub fn effective_lifecycle(record: &WorkRecord, evaluated_at: i64) -> Lifecycle {
    if record.lifecycle == Lifecycle::Leased && !lease_active(record, evaluated_at) {
        Lifecycle::Pending
    } else {
        record.lifecycle
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdministrativeAction {
    Update {
        patch: WorkPatch,
        lifecycle: AdministrativeLifecycle,
        lease_guard: AdministrativeLeaseGuard,
        remove_field_prefixes: BTreeSet<String>,
    },
    Delete {
        lease_guard: AdministrativeLeaseGuard,
    },
}
impl AdministrativeAction {
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        if let Self::Update {
            patch: value,
            remove_field_prefixes,
            ..
        } = self
        {
            patch(value, schema, limits)?;
            for prefix in remove_field_prefixes {
                name(prefix)?;
                if !schema
                    .work
                    .additional_field_prefixes
                    .iter()
                    .any(|namespace| prefix.starts_with(namespace))
                {
                    return Err(invalid(
                        "prefix removal outside provisioned additional namespace",
                    ));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeEntry {
    pub key: String,
    pub expected_version: Option<u64>,
    pub precondition: WorkSelector,
    pub action: AdministrativeAction,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeClause {
    pub id: String,
    pub selector: WorkSelector,
    pub precondition: WorkSelector,
    pub action: AdministrativeAction,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdministrativeOperation {
    Addressed(Vec<AdministrativeEntry>),
    SelectFirst(Vec<AdministrativeClause>),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeIntent {
    pub evaluated_at: i64,
    pub operation: AdministrativeOperation,
    /// None omits before metadata. Some(empty) includes metadata without fields.
    /// Broad controls return aggregate results only; before fields are addressed-only.
    pub before_fields: Option<BTreeSet<String>>,
}
impl AdministrativeIntent {
    /// Structural/count/type/body validation only. The containing operation must
    /// additionally admit serialized and resolved-publication bytes; decoding
    /// must be bounded before this validator receives an owned recursive AST.
    pub fn validate(&self, schema: &QueueSchema, limits: &QueueLimits) -> QueueResult<()> {
        schema.validate()?;
        limits.validate()?;
        if let Some(names) = &self.before_fields {
            for field in names {
                name(field)?;
                if !schema.work.admits_field(field) {
                    return Err(invalid("undeclared before field"));
                }
            }
        }
        match &self.operation {
            AdministrativeOperation::Addressed(entries) => {
                if entries.len() > limits.max_records as usize {
                    return Err(invalid("too many addressed administrative entries"));
                }
                for entry in entries {
                    name(&entry.key)?;
                    entry.precondition.validate(&schema.work)?;
                    entry.action.validate(schema, limits)?;
                }
            }
            AdministrativeOperation::SelectFirst(clauses) => {
                if self.before_fields.is_some() {
                    return Err(invalid("broad administration returns aggregates"));
                }
                for clause in clauses {
                    name(&clause.id)?;
                    clause.selector.validate(&schema.work)?;
                    clause.precondition.validate(&schema.work)?;
                    clause.action.validate(schema, limits)?;
                }
            }
        }
        Ok(())
    }
    /// Freeze ownership on the first matching selector before preconditions.
    pub fn first_clause(&self, record: &WorkRecord) -> Option<usize> {
        let AdministrativeOperation::SelectFirst(clauses) = &self.operation else {
            return None;
        };
        clauses
            .iter()
            .position(|clause| clause.selector.matches(record, self.evaluated_at))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeBefore {
    pub version: u64,
    pub lifecycle: Lifecycle,
    pub available_at: i64,
    pub eligible_since: i64,
    pub attempts: u32,
    pub lease_expires_at: Option<i64>,
    pub fields: Fields,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdministrativeOutcome {
    Changed { version: u64 },
    Deleted { version: u64 },
    NoChange,
    NotFound,
    Conflict { actual_version: u64 },
    Leased,
    LeaseRequired,
    PreconditionFailed,
    Terminal,
    Invalid,
    UniqueConflict,
}
