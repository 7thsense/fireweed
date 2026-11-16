//! The document types: schemas, programs and responses (API-006).

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

// ------------------------------------------------------------------ basics

/// A duration: `250ms`, `30s`, `5m`, `2h`, `7d`, or an integer of
/// milliseconds. Serialized as milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Duration(pub i64);

impl Duration {
    pub fn parse(s: &str) -> Option<Duration> {
        let s = s.trim();
        let split = s.find(|c: char| !c.is_ascii_digit())?;
        let (n, unit) = s.split_at(split);
        let n: i64 = n.parse().ok()?;
        let ms = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return None,
        };
        n.checked_mul(ms).map(Duration)
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Ms(i64),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Ms(n) if n >= 0 => Ok(Duration(n)),
            Raw::Ms(_) => Err(serde::de::Error::custom("a duration can't be negative")),
            Raw::Text(t) => Duration::parse(&t)
                .ok_or_else(|| serde::de::Error::custom(format!("not a duration: {t:?}"))),
        }
    }
}

/// A field's type (API-006 §Documents).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Int,
    Float,
    Bool,
    Timestamp,
    Json,
    TextSet,
}

// ------------------------------------------------------------------ schema

/// A space's schema document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub version: i64,
    #[serde(default = "default_request_retention")]
    pub request_retention: Duration,
    #[serde(default)]
    pub queues: BTreeMap<String, QueueDecl>,
    #[serde(default)]
    pub tables: BTreeMap<String, TableDecl>,
    #[serde(default)]
    pub reads: Vec<ReadDecl>,
}

fn default_request_retention() -> Duration {
    Duration(86_400_000)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaxAttempts {
    Count(u32),
    Unlimited,
}

impl Serialize for MaxAttempts {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            MaxAttempts::Count(n) => s.serialize_u32(*n),
            MaxAttempts::Unlimited => s.serialize_str("unlimited"),
        }
    }
}

impl<'de> Deserialize<'de> for MaxAttempts {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Count(u32),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Count(n) if n > 0 => Ok(MaxAttempts::Count(n)),
            Raw::Text(t) if t == "unlimited" => Ok(MaxAttempts::Unlimited),
            _ => Err(serde::de::Error::custom(
                "max_attempts is a positive integer or \"unlimited\"",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriorityOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDecl {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: MaxAttempts,
    #[serde(default = "default_retention")]
    pub retention: Duration,
    #[serde(default)]
    pub priority: PriorityOrder,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldDecl>,
}

fn default_max_attempts() -> MaxAttempts {
    MaxAttempts::Count(10)
}

fn default_retention() -> Duration {
    Duration(7 * 86_400_000)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDecl {
    #[serde(default)]
    pub key: Vec<String>,
    #[serde(default)]
    pub sequenced: bool,
    #[serde(default)]
    pub expire_after_write: Option<Duration>,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldDecl>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDecl {
    #[serde(rename = "type")]
    pub ty: FieldType,
    #[serde(default)]
    pub index: bool,
    #[serde(default)]
    pub merge: Option<MergeRule>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeRule {
    AddOnly,
    NonDecreasing,
}

/// A named, versioned `select` stored in the schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadDecl {
    pub name: String,
    pub version: u32,
    #[serde(default)]
    pub params: BTreeMap<String, FieldType>,
    pub select: Select,
}

// ----------------------------------------------------------------- program

/// A program: statements run in order as one transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<Durability>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    pub statements: Vec<Statement>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    FireAndForget,
    Tracked,
    Durable,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Statement {
    Insert(Insert),
    Claim(Claim),
    Settle(Settle),
    Renew(Renew),
    Update(Update),
    Delete(Delete),
    Assert(Assert),
    Select(Select),
    Read(ReadCall),
    Pause(QueueRef),
    Resume(QueueRef),
}

impl Statement {
    /// True if the statement changes rows.
    pub fn writes(&self) -> bool {
        !matches!(
            self,
            Statement::Select(_) | Statement::Read(_) | Statement::Assert(_)
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnConflict {
    #[default]
    Skip,
    Reject,
    Update,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Insert {
    pub into: String,
    pub rows: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    pub on_conflict: OnConflict,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    pub from: String,
    pub limit: u32,
    pub lease: Duration,
    pub worker: String,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Pred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eligible_at: Option<Value>,
}

/// An item addressed by id and lease token; each may be a literal or a
/// `{param: …}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemRef {
    pub id: Value,
    pub token: Value,
}

/// How a `settle` ends a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Done: `complete`.
    Complete,
    /// Given up: `failed`.
    Fail,
    /// Back to pending after `after`; failed if attempts are used up.
    Retry,
    /// Back to pending after `after`, with the claim's attempt given back.
    Release,
}

/// Ends leases: `complete`, `fail`, `retry` or `release`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settle {
    pub queue: String,
    pub outcome: Outcome,
    pub items: Vec<ItemRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Duration>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub set: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<Patch>,
    #[serde(default)]
    pub require_all: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Renew {
    pub queue: String,
    pub items: Vec<ItemRef>,
    pub lease: Duration,
    #[serde(default)]
    pub require_all: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeasedPolicy {
    #[default]
    Skip,
    Preserve,
    Break,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnLimit {
    #[default]
    Reject,
    Step,
}

/// What an unmet `expect` does (API-006 §update).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnUnmet {
    /// Reject the whole program with `expect_unmet`.
    #[default]
    Reject,
    /// Change nothing for this statement, report `met: false`, and go on.
    Report,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    #[serde(default)]
    pub on_unmet: OnUnmet,
    #[serde(default)]
    pub min: Option<u64>,
    #[serde(default)]
    pub max: Option<u64>,
}

impl Expect {
    pub fn holds(&self, n: u64) -> bool {
        self.min.is_none_or(|m| n >= m) && self.max.is_none_or(|m| n <= m)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Update {
    pub target: String,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Pred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub set: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<Patch>,
    #[serde(default)]
    pub leased: LeasedPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default)]
    pub on_limit: OnLimit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<Expect>,
    #[serde(default)]
    pub override_merge: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delete {
    pub target: String,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Pred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<Expect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assert {
    pub target: String,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Pred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Value>>,
    pub expect: Expect,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    pub field: String,
    #[serde(default)]
    pub desc: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Select {
    pub from: String,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Pred>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub aggregate: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderBy>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadCall {
    pub name: String,
    pub version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRef {
    pub queue: String,
}

/// A predicate. Comparison operands are raw values: the first is a column
/// name or an operand object, the second a literal or an operand object
/// (API-006 §Operands).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pred {
    And(Vec<Pred>),
    Or(Vec<Pred>),
    Not(Box<Pred>),
    Eq(Vec<Value>),
    Ne(Vec<Value>),
    Lt(Vec<Value>),
    Le(Vec<Value>),
    Gt(Vec<Value>),
    Ge(Vec<Value>),
    In(Vec<Value>),
    Contains(Vec<Value>),
    IsNull(Value),
    NotNull(Value),
}

/// A body patch (API-006 §Body patches).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Patch {
    Set(PatchSet),
    Remove(PatchRemove),
    Append(PatchAppend),
    Replace(PatchReplace),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchSet {
    pub set: String,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchRemove {
    pub remove: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchAppend {
    pub append: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_last: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchReplace {
    pub replace: Value,
}

// ---------------------------------------------------------------- response

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Committed,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    KeyConflict,
    StaleLease,
    LimitExceeded,
    ExpectUnmet,
    MergeViolation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    /// Index of the statement that rejected.
    pub statement: usize,
    pub reason: RejectReason,
}

/// A program's response. Each result is the JSON API-006 gives for its
/// statement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub request_id: Option<String>,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected: Option<Rejection>,
    pub results: Vec<Value>,
    pub durability: Durability,
    #[serde(default)]
    pub replayed: bool,
}

/// The result of `apply_schema`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applied {
    pub version: i64,
    pub changed: bool,
}
