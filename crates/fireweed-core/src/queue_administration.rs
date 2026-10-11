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
/// An unshared postfix tree. Ownership, serialization and destruction are flat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkSelector {
    pub nodes: Vec<SelectorNode>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectorNode {
    All(u32),
    Any(u32),
    Not,
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
    pub fn leaf(node: SelectorNode) -> Self {
        Self { nodes: vec![node] }
    }
    pub fn all(children: Vec<Self>) -> Self {
        Self::fold(children, true)
    }
    pub fn any(children: Vec<Self>) -> Self {
        Self::fold(children, false)
    }
    pub fn negate(mut child: Self) -> Self {
        child.nodes.push(SelectorNode::Not);
        child
    }
    fn fold(children: Vec<Self>, all: bool) -> Self {
        let arity = u32::try_from(children.len()).expect("selector arity exceeds u32");
        let mut nodes = Vec::new();
        for child in children {
            nodes.extend(child.nodes);
        }
        nodes.push(if all {
            SelectorNode::All(arity)
        } else {
            SelectorNode::Any(arity)
        });
        Self { nodes }
    }
    fn evaluate(&self, mut leaf: impl FnMut(&SelectorNode) -> bool) -> QueueResult<bool> {
        // Scalar postfix admission catches underflow and disconnected roots.
        let mut operands = 0usize;
        for node in &self.nodes {
            let arity = match node {
                SelectorNode::All(n) | SelectorNode::Any(n) => *n as usize,
                SelectorNode::Not => 1,
                _ => 0,
            };
            operands = operands
                .checked_sub(arity)
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| invalid("invalid selector operand arity"))?;
        }
        if operands != 1 {
            return Err(invalid("selector must have exactly one root"));
        }
        #[derive(Clone, Copy)]
        struct Frame {
            remaining: u32,
            all: bool,
            negate: bool,
            value: bool,
        }
        // Reverse postfix is prefix with reversed children. Boolean folds are
        // commutative; no leaf is skipped. Scratch depends on depth, not width.
        let mut frames: [Option<Frame>; MAX_SELECTOR_DEPTH] = [None; MAX_SELECTOR_DEPTH];
        let mut depth = 0usize;
        let mut result = false;
        for node in self.nodes.iter().rev() {
            if depth >= MAX_SELECTOR_DEPTH {
                return Err(invalid("selector nesting exceeds common limit"));
            }
            let operator = match node {
                SelectorNode::All(n) => Some(Frame {
                    remaining: *n,
                    all: true,
                    negate: false,
                    value: true,
                }),
                SelectorNode::Any(n) => Some(Frame {
                    remaining: *n,
                    all: false,
                    negate: false,
                    value: false,
                }),
                SelectorNode::Not => Some(Frame {
                    remaining: 1,
                    all: true,
                    negate: true,
                    value: true,
                }),
                _ => None,
            };
            let mut value = if let Some(frame) = operator {
                if frame.remaining != 0 {
                    frames[depth] = Some(frame);
                    depth += 1;
                    continue;
                }
                frame.value
            } else {
                leaf(node)
            };
            loop {
                if depth == 0 {
                    result = value;
                    break;
                }
                let frame = frames[depth - 1].as_mut().expect("occupied selector frame");
                frame.value = if frame.all {
                    frame.value & value
                } else {
                    frame.value | value
                };
                frame.remaining -= 1;
                if frame.remaining != 0 {
                    break;
                }
                value = if frame.negate {
                    !frame.value
                } else {
                    frame.value
                };
                frames[depth - 1] = None;
                depth -= 1;
            }
        }
        if depth != 0 {
            return Err(invalid("incomplete selector tree"));
        }
        Ok(result)
    }
    pub fn validate(&self, definition: &CollectionDefinition) -> QueueResult<()> {
        self.evaluate(|_| false)?;
        for node in &self.nodes {
            match node {
                SelectorNode::Field { name, predicate } => {
                    validate_field_predicate(definition, name, predicate)?
                }
                SelectorNode::Present { name: field, .. } => {
                    name(field)?;
                    if !definition.admits_field(field) {
                        return Err(invalid("undeclared selector field"));
                    }
                }
                SelectorNode::Keys(keys) => {
                    for key in keys {
                        name(key)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// Reference oracle only: physical stores select natively, never claim
    /// broadly and filter on the client. Malformed programs fail closed.
    pub fn matches(&self, record: &WorkRecord, evaluated_at: i64) -> bool {
        self.evaluate(|node| match node {
            SelectorNode::Field { name, predicate } => {
                predicate.matches(record.work.fields.get(name))
            }
            SelectorNode::Present { name, present } => {
                record.work.fields.contains_key(name) == *present
            }
            SelectorNode::Keys(keys) => keys.contains(&record.work.key),
            SelectorNode::StoredLifecycleIn(states) => states.contains(&record.lifecycle),
            SelectorNode::EffectiveLifecycleIn(states) => {
                states.contains(&effective_lifecycle(record, evaluated_at))
            }
            SelectorNode::Attempts { comparison, value } => {
                comparison.matches(record.attempts, *value)
            }
            SelectorNode::AvailableAt { comparison, value } => {
                comparison.matches(record.work.available_at, *value)
            }
            SelectorNode::LeaseActive(expected) => lease_active(record, evaluated_at) == *expected,
            SelectorNode::All(_) | SelectorNode::Any(_) | SelectorNode::Not => {
                unreachable!("operators handled by evaluator")
            }
        })
        .unwrap_or(false)
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
    /// must be bounded before this validator receives an owned flat program.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AdministrativeOutcomeKind {
    Changed,
    Deleted,
    NoChange,
    NotFound,
    Conflict,
    Leased,
    LeaseRequired,
    PreconditionFailed,
    Terminal,
    Invalid,
    UniqueConflict,
}
impl AdministrativeOutcome {
    pub fn kind(&self) -> AdministrativeOutcomeKind {
        match self {
            Self::Changed { .. } => AdministrativeOutcomeKind::Changed,
            Self::Deleted { .. } => AdministrativeOutcomeKind::Deleted,
            Self::NoChange => AdministrativeOutcomeKind::NoChange,
            Self::NotFound => AdministrativeOutcomeKind::NotFound,
            Self::Conflict { .. } => AdministrativeOutcomeKind::Conflict,
            Self::Leased => AdministrativeOutcomeKind::Leased,
            Self::LeaseRequired => AdministrativeOutcomeKind::LeaseRequired,
            Self::PreconditionFailed => AdministrativeOutcomeKind::PreconditionFailed,
            Self::Terminal => AdministrativeOutcomeKind::Terminal,
            Self::Invalid => AdministrativeOutcomeKind::Invalid,
            Self::UniqueConflict => AdministrativeOutcomeKind::UniqueConflict,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdministrativeHistogram {
    /// All addressed positions count, including missing/duplicate targets.
    pub matched: u64,
    pub outcomes: std::collections::BTreeMap<AdministrativeOutcomeKind, u64>,
}
impl AdministrativeHistogram {
    pub fn record(&mut self, outcome: &AdministrativeOutcome) -> QueueResult<()> {
        let kind = outcome.kind();
        let count = self
            .outcomes
            .get(&kind)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| QueueError::OutcomeUnknown("administrative count exhausted".into()))?;
        let matched = self
            .matched
            .checked_add(1)
            .ok_or_else(|| QueueError::OutcomeUnknown("administrative count exhausted".into()))?;
        self.outcomes.insert(kind, count);
        self.matched = matched;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeItemResult {
    pub key: String,
    pub outcome: AdministrativeOutcome,
    pub before: Option<AdministrativeBefore>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdministrativeClauseResult {
    pub id: String,
    pub histogram: AdministrativeHistogram,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeResult {
    pub histogram: AdministrativeHistogram,
    pub items: Vec<AdministrativeItemResult>,
    pub clauses: Vec<AdministrativeClauseResult>,
}
