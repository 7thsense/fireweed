//! Checks schemas and resolves programs against them (API-008).
//!
//! [`schema`] validates a schema, [`change`] checks that a new schema only
//! adds to an old one, and [`resolve`] checks a program and returns typed
//! statements with every parameter replaced by its value.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::*;

/// Program limits (API-008 §Limits).
pub const MAX_STATEMENTS: usize = 64;
pub const MAX_ROWS: usize = 1_000;
pub const MAX_CLAIM: u32 = 1_000;
pub const MAX_UPDATE: u32 = 10_000;
pub const MAX_SELECT: u32 = 10_000;
pub const DEFAULT_UPDATE_LIMIT: u32 = 1_000;
pub const MAX_PROGRAM_BYTES: usize = 8 << 20;
/// Programs per `execute_batch` (API-008 §Limits).
pub const MAX_BATCH: usize = 64;

// ------------------------------------------------------------------ names

pub fn valid_name(n: &str) -> bool {
    let b = n.as_bytes();
    !b.is_empty()
        && b.len() <= 48
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

/// A queue's system columns.
pub const QUEUE_COLUMNS: &[(&str, FieldType)] = &[
    ("id", FieldType::Int),
    ("key", FieldType::Text),
    ("state", FieldType::Text),
    ("priority", FieldType::Int),
    ("due_at", FieldType::Timestamp),
    ("attempts", FieldType::Int),
    ("version", FieldType::Int),
    ("lease_owner", FieldType::Text),
    ("created_at", FieldType::Timestamp),
    ("updated_at", FieldType::Timestamp),
    ("finished_at", FieldType::Timestamp),
    ("body", FieldType::Json),
];

/// A table's system columns.
pub const TABLE_COLUMNS: &[(&str, FieldType)] = &[
    ("id", FieldType::Int),
    ("version", FieldType::Int),
    ("created_at", FieldType::Timestamp),
    ("updated_at", FieldType::Timestamp),
    ("expires_at", FieldType::Timestamp),
];

// ----------------------------------------------------------------- schema

/// Validates a schema on its own.
pub fn schema(s: &Schema) -> Result<()> {
    if s.version < 1 {
        return invalid("schema version starts at 1");
    }
    for name in s.queues.keys().chain(s.tables.keys()) {
        if !valid_name(name) {
            return invalid(format!("object name {name:?}"));
        }
    }
    if let Some(n) = s.queues.keys().find(|n| s.tables.contains_key(*n)) {
        return invalid(format!("{n:?} is both a queue and a table"));
    }
    for (qn, q) in &s.queues {
        fields(qn, &q.fields, QUEUE_COLUMNS)?;
        if q.fields.values().any(|f| f.merge.is_some()) {
            return invalid(format!("queue {qn}: merge rules are for table fields"));
        }
    }
    for (tn, t) in &s.tables {
        fields(tn, &t.fields, TABLE_COLUMNS)?;
        if t.sequenced && !t.key.is_empty() {
            return invalid(format!("table {tn}: a sequenced table has no key"));
        }
        if !t.sequenced && t.key.is_empty() {
            return invalid(format!("table {tn}: a key is required"));
        }
        let mut seen = BTreeSet::new();
        for k in &t.key {
            let Some(f) = t.fields.get(k) else {
                return invalid(format!("table {tn}: key field {k} is not declared"));
            };
            if matches!(f.ty, FieldType::Json | FieldType::TextSet) || !seen.insert(k) {
                return invalid(format!("table {tn}: key field {k}"));
            }
        }
    }
    let mut reads = BTreeSet::new();
    for r in &s.reads {
        if !valid_name(&r.name) || !reads.insert((&r.name, r.version)) {
            return invalid(format!("read {} version {}", r.name, r.version));
        }
        // Check the read's select with a stand-in value for each parameter.
        let params = r
            .params
            .iter()
            .map(|(k, t)| (k.clone(), sample(*t)))
            .collect();
        resolve_select(s, &r.select, &params)
            .map_err(|e| Error::Invalid(format!("read {}: {e}", r.name)))?;
    }
    Ok(())
}

fn fields(obj: &str, fs: &BTreeMap<String, FieldDecl>, system: &[(&str, FieldType)]) -> Result<()> {
    for (n, f) in fs {
        if !valid_name(n) || system.iter().any(|(s, _)| s == n) {
            return invalid(format!("{obj}: field name {n:?}"));
        }
        let ok = match f.merge {
            None => true,
            Some(MergeRule::AddOnly) => f.ty == FieldType::TextSet,
            Some(MergeRule::NonDecreasing) => {
                matches!(
                    f.ty,
                    FieldType::Int | FieldType::Float | FieldType::Timestamp
                )
            }
        };
        if !ok {
            return invalid(format!("{obj}.{n}: merge rule doesn't fit its type"));
        }
        if f.index && f.ty == FieldType::Json {
            return invalid(format!("{obj}.{n}: a json field can't be indexed"));
        }
    }
    Ok(())
}

fn sample(t: FieldType) -> Value {
    match t {
        FieldType::Text => Value::from(""),
        FieldType::Int => Value::from(0),
        FieldType::Float => Value::from(0.0),
        FieldType::Bool => Value::from(false),
        FieldType::Timestamp => Value::from("2000-01-01T00:00:00Z"),
        FieldType::Json => Value::Null,
        FieldType::TextSet => Value::Array(vec![]),
    }
}

/// Checks that `new` only adds to `old` (API-008 §Changing a schema).
pub fn change(old: &Schema, new: &Schema) -> Result<()> {
    if new.version <= old.version {
        return invalid("a changed schema needs a higher version");
    }
    if new.request_retention < old.request_retention {
        return invalid("request_retention can't shrink");
    }
    for (n, o) in &old.queues {
        let Some(q) = new.queues.get(n) else {
            return invalid(format!("queue {n} removed"));
        };
        if q.max_attempts != o.max_attempts || q.priority != o.priority || q.retention < o.retention
        {
            return invalid(format!(
                "queue {n}: only retention may change, and only grow"
            ));
        }
        kept_fields(n, &o.fields, &q.fields)?;
    }
    for (n, o) in &old.tables {
        let Some(t) = new.tables.get(n) else {
            return invalid(format!("table {n} removed"));
        };
        if t.key != o.key
            || t.sequenced != o.sequenced
            || t.expire_after_write != o.expire_after_write
        {
            return invalid(format!(
                "table {n}: key, sequencing and expiry can't change"
            ));
        }
        kept_fields(n, &o.fields, &t.fields)?;
    }
    for r in &old.reads {
        if !new.reads.contains(r) {
            return invalid(format!(
                "read {} version {} changed or removed",
                r.name, r.version
            ));
        }
    }
    Ok(())
}

fn kept_fields(
    obj: &str,
    old: &BTreeMap<String, FieldDecl>,
    new: &BTreeMap<String, FieldDecl>,
) -> Result<()> {
    // An index on a live object would block its writes while it builds, so
    // indexes come only with new objects.
    for (n, o) in old {
        match new.get(n) {
            Some(f) if f == o => {}
            _ => return invalid(format!("{obj}.{n}: an existing field can't change")),
        }
    }
    if let Some(n) = new.keys().find(|n| !old.contains_key(*n) && new[*n].index) {
        return invalid(format!(
            "{obj}.{n}: a new field on an existing object can't be indexed"
        ));
    }
    Ok(())
}

// --------------------------------------------------------- resolved forms

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Queue,
    Table,
}

/// The object a statement addresses.
#[derive(Clone, Debug, PartialEq)]
pub struct Obj {
    pub name: String,
    pub kind: Kind,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub system: bool,
    pub ty: FieldType,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operand {
    Column(Column),
    /// A literal or a parameter's value; may be null.
    Literal(Value, FieldType),
    /// The request time plus an offset in milliseconds.
    Now(i64),
    /// A value inside a queue item's body.
    Path(Vec<String>, FieldType),
}

impl Operand {
    pub fn ty(&self) -> FieldType {
        match self {
            Operand::Column(c) => c.ty,
            Operand::Literal(_, t) | Operand::Path(_, t) => *t,
            Operand::Now(_) => FieldType::Timestamp,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RPred {
    And(Vec<RPred>),
    Or(Vec<RPred>),
    Not(Box<RPred>),
    Cmp(CmpOp, Operand, Operand),
    In(Operand, Vec<Operand>),
    Contains(Operand, Operand),
    IsNull(Operand),
    NotNull(Operand),
    /// Rows of another object match: each `(inner column, outer operand)`
    /// pair is an equality, and `filter` is over the inner object.
    Exists {
        obj: Obj,
        matches: Vec<(Column, Operand)>,
        filter: Option<Box<RPred>>,
    },
}

/// The value assigned to a field.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Operand(Operand),
    Add(Operand, Operand),
    Sub(Operand, Operand),
    Coalesce(Operand, Operand),
    /// Add these values to the field's own set.
    SetAdd(Operand),
    /// Remove these values from the field's own set.
    SetRemove(Operand),
}

#[derive(Clone, Debug, PartialEq)]
pub enum RPatch {
    Set(Vec<String>, Operand),
    Remove(Vec<String>),
    Append(Vec<String>, Operand, Option<u32>),
    Replace(Operand),
}

/// Rows addressed by a write or guard.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Selection {
    pub filter: Option<RPred>,
    pub ids: Option<Vec<i64>>,
    /// Queue client keys.
    pub queue_keys: Option<Vec<String>>,
    /// Table keys: one value per key field, in key order.
    pub table_keys: Option<Vec<Vec<Operand>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Agg {
    Count,
    Sum(Column),
    Min(Column),
    Max(Column),
    Avg(Column),
    Percentile(Column, f64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Output {
    Fields(Vec<Column>),
    Aggregate {
        aggs: Vec<(String, Agg)>,
        group_by: Vec<Column>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RSelect {
    pub obj: Obj,
    pub filter: Option<RPred>,
    pub output: Output,
    /// Output name, descending.
    pub order_by: Vec<(String, bool)>,
    pub limit: u32,
}

/// A queue row to insert. Values are literals.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct QueueRow {
    pub key: Option<String>,
    pub priority: i64,
    pub due: Option<String>,
    pub after_ms: i64,
    pub body: Option<Value>,
    pub fields: BTreeMap<String, Value>,
}

pub use crate::Outcome;

#[derive(Clone, Debug, PartialEq)]
pub enum RStatement {
    InsertQueue {
        obj: Obj,
        rows: Vec<QueueRow>,
        on_conflict: OnConflict,
    },
    InsertTable {
        obj: Obj,
        rows: Vec<BTreeMap<String, Value>>,
        on_conflict: OnConflict,
    },
    Claim {
        obj: Obj,
        limit: u32,
        lease_ms: i64,
        worker: String,
        filter: Option<RPred>,
        eligible_at: Option<Operand>,
    },
    Settle {
        obj: Obj,
        outcome: Outcome,
        items: Vec<(i64, String)>,
        after_ms: i64,
        set: Vec<(Column, Expr)>,
        body: Vec<RPatch>,
        require_all: bool,
    },
    Renew {
        obj: Obj,
        items: Vec<(i64, String)>,
        lease_ms: i64,
        require_all: bool,
    },
    Update {
        obj: Obj,
        sel: Selection,
        set: Vec<(Column, Expr)>,
        body: Vec<RPatch>,
        leased: LeasedPolicy,
        limit: u32,
        on_limit: OnLimit,
        expect: Option<Expect>,
        override_merge: bool,
    },
    Delete {
        obj: Obj,
        sel: Selection,
        limit: u32,
        expect: Option<Expect>,
    },
    Assert {
        obj: Obj,
        sel: Selection,
        expect: Expect,
    },
    Select(RSelect),
    SetPaused {
        obj: Obj,
        paused: bool,
    },
}

/// A checked program.
#[derive(Clone, Debug, PartialEq)]
pub struct Resolved {
    pub statements: Vec<RStatement>,
    /// True if any statement writes.
    pub writes: bool,
    /// SHA-256 of the program's canonical JSON, hex.
    pub fingerprint: String,
}

// -------------------------------------------------------------- resolving

/// SHA-256 of a program's canonical JSON, hex: what a request record keeps
/// to tell a replay from a different program under the same id.
pub fn fingerprint(p: &Program) -> Result<String> {
    use sha2::Digest;
    let canonical = serde_json::to_vec(p).map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(sha2::Sha256::digest(&canonical)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Checks `p` against `s` and resolves it.
pub fn resolve(s: &Schema, p: &Program) -> Result<Resolved> {
    let canonical = serde_json::to_vec(p).map_err(|e| Error::Invalid(e.to_string()))?;
    if canonical.len() > MAX_PROGRAM_BYTES {
        return invalid("program larger than 8 MiB");
    }
    if p.statements.is_empty() || p.statements.len() > MAX_STATEMENTS {
        return invalid("a program has 1 to 64 statements");
    }
    let writes = p.statements.iter().any(Statement::writes);
    if writes && p.request_id.as_deref().is_none_or(str::is_empty) {
        return invalid("a program that writes needs a request_id");
    }
    if p.request_id.is_some() && p.issued_at.is_none() {
        return invalid("a request_id needs issued_at");
    }
    let mut statements = vec![];
    let mut sequenced = false;
    for (i, st) in p.statements.iter().enumerate() {
        let r = statement(s, st, &p.params).map_err(|e| match e {
            Error::Invalid(m) => Error::Invalid(format!("statement {i}: {m}")),
            other => other,
        })?;
        // A sequenced insert holds its table's counter row until commit, so it
        // comes after every other write (API-008 §Tables).
        let is_seq =
            matches!(&r, RStatement::InsertTable { obj, .. } if s.tables[&obj.name].sequenced);
        if sequenced && st.writes() && !is_seq {
            return invalid(format!(
                "statement {i}: writes after an insert into a sequenced table"
            ));
        }
        sequenced |= is_seq;
        statements.push(r);
    }
    let fingerprint = fingerprint(p)?;
    Ok(Resolved {
        statements,
        writes,
        fingerprint,
    })
}

type Params = BTreeMap<String, Value>;

/// Name resolution for one object.
struct Ctx<'a> {
    schema: &'a Schema,
    obj: Obj,
    queue: Option<&'a QueueDecl>,
    table: Option<&'a TableDecl>,
    params: &'a Params,
}

fn ctx<'a>(s: &'a Schema, name: &str, params: &'a Params) -> Result<Ctx<'a>> {
    if let Some(q) = s.queues.get(name) {
        Ok(Ctx {
            schema: s,
            obj: Obj {
                name: name.into(),
                kind: Kind::Queue,
            },
            queue: Some(q),
            table: None,
            params,
        })
    } else if let Some(t) = s.tables.get(name) {
        Ok(Ctx {
            schema: s,
            obj: Obj {
                name: name.into(),
                kind: Kind::Table,
            },
            queue: None,
            table: Some(t),
            params,
        })
    } else {
        invalid(format!("no queue or table {name:?}"))
    }
}

impl Ctx<'_> {
    fn declared(&self) -> &BTreeMap<String, FieldDecl> {
        match (self.queue, self.table) {
            (Some(q), _) => &q.fields,
            (_, Some(t)) => &t.fields,
            _ => unreachable!(),
        }
    }

    fn column(&self, name: &str) -> Result<Column> {
        let system = match self.obj.kind {
            Kind::Queue => QUEUE_COLUMNS,
            Kind::Table => TABLE_COLUMNS,
        };
        if let Some((n, t)) = system.iter().find(|(n, _)| *n == name) {
            return Ok(Column {
                name: (*n).into(),
                system: true,
                ty: *t,
            });
        }
        match self.declared().get(name) {
            Some(f) => Ok(Column {
                name: name.into(),
                system: false,
                ty: f.ty,
            }),
            None => invalid(format!("{} has no field {name:?}", self.obj.name)),
        }
    }

    fn queue(&self) -> Result<&QueueDecl> {
        self.queue
            .ok_or_else(|| Error::Invalid(format!("{} is not a queue", self.obj.name)))
    }

    /// Columns with an index (API-008 §Index rule).
    fn indexed(&self, c: &Column) -> bool {
        match self.obj.kind {
            Kind::Queue => {
                (c.system && matches!(c.name.as_str(), "id" | "key"))
                    || (!c.system && self.declared().get(&c.name).is_some_and(|f| f.index))
            }
            Kind::Table => {
                let t = self.table.expect("table");
                (c.system && c.name == "id")
                    || t.key.first() == Some(&c.name)
                    || (!c.system && t.fields.get(&c.name).is_some_and(|f| f.index))
            }
        }
    }

    fn param(&self, name: &Value) -> Result<Value> {
        let n = name
            .as_str()
            .ok_or_else(|| Error::Invalid("param names are strings".into()))?;
        self.params
            .get(n)
            .cloned()
            .ok_or_else(|| Error::Invalid(format!("no param {n:?}")))
    }

    /// An operand that carries its own type, or `None` if it needs one.
    fn typed(&self, v: &Value, first: bool) -> Result<Option<Operand>> {
        if first && let Value::String(name) = v {
            return Ok(Some(Operand::Column(self.column(name)?)));
        }
        let Value::Object(m) = v else { return Ok(None) };
        if let Some(f) = m.get("field") {
            if m.len() != 1 {
                return invalid("{field: …} takes no other keys");
            }
            let name = f
                .as_str()
                .ok_or_else(|| Error::Invalid("field names are strings".into()))?;
            return Ok(Some(Operand::Column(self.column(name)?)));
        }
        if let Some(n) = m.get("now") {
            if m.len() != 1 {
                return invalid("{now: …} takes no other keys");
            }
            let off = match n {
                Value::Object(o) if o.is_empty() => 0,
                Value::Null => 0,
                Value::String(s) => match s.strip_prefix('-') {
                    Some(rest) => {
                        -Duration::parse(rest)
                            .ok_or_else(|| Error::Invalid(format!("offset {s:?}")))?
                            .0
                    }
                    None => {
                        Duration::parse(s.trim_start_matches('+'))
                            .ok_or_else(|| Error::Invalid(format!("offset {s:?}")))?
                            .0
                    }
                },
                _ => return invalid("now takes {} or an offset like \"-5m\""),
            };
            return Ok(Some(Operand::Now(off)));
        }
        if let Some(p) = m.get("path") {
            if self.obj.kind != Kind::Queue {
                return invalid("paths read a queue item's body");
            }
            let ty: FieldType = m
                .get("type")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| Error::Invalid(e.to_string()))?
                .ok_or_else(|| Error::Invalid("a path needs a type".into()))?;
            if m.len() != 2 {
                return invalid("{path, type} takes no other keys");
            }
            return Ok(Some(Operand::Path(path(p)?, ty)));
        }
        Ok(None)
    }

    /// An operand of a known type.
    fn operand(&self, v: &Value, ty: FieldType) -> Result<Operand> {
        if let Some(o) = self.typed(v, false)? {
            if o.ty() != ty {
                return invalid(format!("{o:?} is not a {ty:?}"));
            }
            return Ok(o);
        }
        let lit = match v {
            Value::Object(m) if m.len() == 1 && m.contains_key("param") => {
                self.param(&m["param"])?
            }
            Value::Object(m) if m.len() == 1 && m.contains_key("value") => m["value"].clone(),
            other => other.clone(),
        };
        literal(&lit, ty)?;
        Ok(Operand::Literal(lit, ty))
    }

    fn pred(&self, p: &Pred) -> Result<RPred> {
        Ok(match p {
            Pred::And(ps) => RPred::And(ps.iter().map(|p| self.pred(p)).collect::<Result<_>>()?),
            Pred::Or(ps) => RPred::Or(ps.iter().map(|p| self.pred(p)).collect::<Result<_>>()?),
            Pred::Not(p) => RPred::Not(Box::new(self.pred(p)?)),
            Pred::Eq(a) => self.cmp(CmpOp::Eq, a)?,
            Pred::Ne(a) => self.cmp(CmpOp::Ne, a)?,
            Pred::Lt(a) => self.cmp(CmpOp::Lt, a)?,
            Pred::Le(a) => self.cmp(CmpOp::Le, a)?,
            Pred::Gt(a) => self.cmp(CmpOp::Gt, a)?,
            Pred::Ge(a) => self.cmp(CmpOp::Ge, a)?,
            Pred::In(a) => {
                let [x, list] = two(a)?;
                let x = self
                    .typed(x, true)?
                    .ok_or_else(|| Error::Invalid("in: the first operand is a column".into()))?;
                let values = match list {
                    Value::Array(vs) => vs.clone(),
                    Value::Object(m) if m.len() == 1 && m.contains_key("param") => {
                        match self.param(&m["param"])? {
                            Value::Array(vs) => vs,
                            _ => return invalid("in: the param must be an array"),
                        }
                    }
                    _ => return invalid("in: the second operand is a list or a {param}"),
                };
                if values.len() > 1_000 {
                    return invalid("in: at most 1000 values");
                }
                let t = x.ty();
                RPred::In(
                    x,
                    values
                        .iter()
                        .map(|v| self.operand(v, t))
                        .collect::<Result<_>>()?,
                )
            }
            Pred::Contains(a) => {
                let [x, v] = two(a)?;
                let x = self.typed(x, true)?.ok_or_else(|| {
                    Error::Invalid("contains: the first operand is a column".into())
                })?;
                if x.ty() != FieldType::TextSet {
                    return invalid("contains needs a text_set");
                }
                RPred::Contains(x, self.operand(v, FieldType::Text)?)
            }
            Pred::IsNull(v) => RPred::IsNull(
                self.typed(v, true)?
                    .ok_or_else(|| Error::Invalid("is_null takes a column".into()))?,
            ),
            Pred::NotNull(v) => RPred::NotNull(
                self.typed(v, true)?
                    .ok_or_else(|| Error::Invalid("not_null takes a column".into()))?,
            ),
            Pred::Exists(l) => {
                let inner = ctx(self.schema, &l.object, self.params)?;
                if l.matches.is_empty() {
                    return invalid("exists needs at least one match");
                }
                let mut matches = vec![];
                for (k, v) in &l.matches {
                    let col = inner.column(k)?;
                    matches.push((col.clone(), self.operand(v, col.ty)?));
                }
                // The lookup itself must use an index of the inner object.
                if !matches.iter().any(|(c, _)| inner.indexed(c)) {
                    return invalid(format!(
                        "exists in {}: match an indexed field (API-008 §Index rule)",
                        l.object
                    ));
                }
                let filter = l
                    .filter
                    .as_ref()
                    .map(|f| inner.pred(f))
                    .transpose()?
                    .map(Box::new);
                RPred::Exists {
                    obj: inner.obj,
                    matches,
                    filter,
                }
            }
        })
    }

    fn cmp(&self, op: CmpOp, args: &[Value]) -> Result<RPred> {
        let [a, b] = two(args)?;
        let (a, b) = match (self.typed(a, true)?, self.typed(b, false)?) {
            (Some(a), Some(b)) => (a, b),
            (Some(a), None) => {
                let t = a.ty();
                (a, self.operand(b, t)?)
            }
            (None, Some(b)) => (self.operand(a, b.ty())?, b),
            (None, None) => return invalid("a comparison needs a column, path or now"),
        };
        if a.ty() != b.ty() {
            return invalid(format!("comparing {:?} with {:?}", a.ty(), b.ty()));
        }
        if !matches!(op, CmpOp::Eq | CmpOp::Ne)
            && matches!(
                a.ty(),
                FieldType::Bool | FieldType::Json | FieldType::TextSet
            )
        {
            return invalid(format!("{:?} values have no order", a.ty()));
        }
        Ok(RPred::Cmp(op, a, b))
    }

    /// `set` assignments to declared fields.
    /// `set` assignments. `schedule` lets an `update` on unleased queue items
    /// set `priority`, `due_at` and `state` (API-008 §update).
    fn assignments(
        &self,
        set: &BTreeMap<String, Value>,
        schedule: bool,
    ) -> Result<Vec<(Column, Expr)>> {
        let mut out = vec![];
        for (name, v) in set {
            let c = self.column(name)?;
            if c.system {
                let ok = schedule && matches!(name.as_str(), "priority" | "due_at" | "state");
                if !ok {
                    return invalid(format!("{name} is a system column"));
                }
                if name == "state" {
                    let st = match self.operand(v, FieldType::Text)? {
                        Operand::Literal(Value::String(st), _) => st,
                        _ => return invalid("state is set to a literal"),
                    };
                    if !matches!(st.as_str(), "pending" | "complete" | "failed") {
                        return invalid("state is set to pending, complete or failed");
                    }
                    out.push((
                        c,
                        Expr::Operand(Operand::Literal(Value::String(st), FieldType::Text)),
                    ));
                    continue;
                }
            }
            let t = c.ty;
            let e = match v {
                Value::Object(m) if m.len() == 1 => {
                    let (k, arg) = m.iter().next().expect("one key");
                    match k.as_str() {
                        "add" | "sub" | "coalesce" => {
                            let [x, y] = two(arg.as_array().map(Vec::as_slice).unwrap_or(&[]))?;
                            let (x, y) = (self.operand(x, t)?, self.operand(y, t)?);
                            if k != "coalesce" && !matches!(t, FieldType::Int | FieldType::Float) {
                                return invalid(format!("{k} needs a number"));
                            }
                            match k.as_str() {
                                "add" => Expr::Add(x, y),
                                "sub" => Expr::Sub(x, y),
                                _ => Expr::Coalesce(x, y),
                            }
                        }
                        "set_add" | "set_remove" => {
                            if t != FieldType::TextSet {
                                return invalid(format!("{k} needs a text_set field"));
                            }
                            let vals = self.operand(arg, FieldType::TextSet)?;
                            if k == "set_add" {
                                Expr::SetAdd(vals)
                            } else {
                                Expr::SetRemove(vals)
                            }
                        }
                        _ => Expr::Operand(self.operand(v, t)?),
                    }
                }
                _ => Expr::Operand(self.operand(v, t)?),
            };
            out.push((c, e));
        }
        Ok(out)
    }

    fn patches(&self, ps: &[Patch]) -> Result<Vec<RPatch>> {
        if !ps.is_empty() {
            self.queue()?;
        }
        ps.iter()
            .map(|p| {
                Ok(match p {
                    Patch::Set(s) => RPatch::Set(
                        path(&Value::from(s.set.clone()))?,
                        self.operand(&s.value, FieldType::Json)?,
                    ),
                    Patch::Remove(r) => RPatch::Remove(path(&Value::from(r.remove.clone()))?),
                    Patch::Append(a) => RPatch::Append(
                        path(&Value::from(a.append.clone()))?,
                        self.operand(&a.value, FieldType::Json)?,
                        a.keep_last,
                    ),
                    Patch::Replace(r) => {
                        RPatch::Replace(self.operand(&r.replace, FieldType::Json)?)
                    }
                })
            })
            .collect()
    }

    fn items(&self, items: &[ItemRef]) -> Result<Vec<(i64, String)>> {
        if items.is_empty() || items.len() > MAX_ROWS {
            return invalid("1 to 1000 items");
        }
        items
            .iter()
            .map(|i| {
                let id = match self.operand(&i.id, FieldType::Int)? {
                    Operand::Literal(Value::Number(n), _) => n.as_i64().expect("checked int"),
                    _ => return invalid("an item id is a literal or a param"),
                };
                let token = match self.operand(&i.token, FieldType::Text)? {
                    Operand::Literal(Value::String(s), _) => s,
                    _ => return invalid("a token is a literal or a param"),
                };
                Ok((id, token))
            })
            .collect()
    }

    fn selection(
        &self,
        filter: &Option<Pred>,
        ids: &Option<Vec<Value>>,
        keys: &Option<Vec<Value>>,
    ) -> Result<Selection> {
        let mut sel = Selection {
            filter: filter.as_ref().map(|p| self.pred(p)).transpose()?,
            ..Default::default()
        };
        if let Some(ids) = ids {
            sel.ids = Some(
                ids.iter()
                    .map(|v| match self.operand(v, FieldType::Int)? {
                        Operand::Literal(Value::Number(n), _) => {
                            Ok(n.as_i64().expect("checked int"))
                        }
                        _ => invalid("ids are literals or params"),
                    })
                    .collect::<Result<_>>()?,
            );
        }
        if let Some(keys) = keys {
            match (self.queue, self.table) {
                (Some(_), _) => {
                    sel.queue_keys = Some(
                        keys.iter()
                            .map(|v| match self.operand(v, FieldType::Text)? {
                                Operand::Literal(Value::String(s), _) => Ok(s),
                                _ => invalid("queue keys are strings"),
                            })
                            .collect::<Result<_>>()?,
                    )
                }
                (_, Some(t)) => {
                    sel.table_keys = Some(
                        keys.iter()
                            .map(|k| {
                                let Value::Object(m) = k else {
                                    return invalid("a table key is an object of key fields");
                                };
                                if m.len() != t.key.len() {
                                    return invalid("a table key names every key field");
                                }
                                t.key
                                    .iter()
                                    .map(|f| {
                                        let v = m.get(f).ok_or_else(|| {
                                            Error::Invalid(format!("key field {f} missing"))
                                        })?;
                                        self.operand(v, t.fields[f].ty)
                                    })
                                    .collect()
                            })
                            .collect::<Result<_>>()?,
                    )
                }
                _ => unreachable!(),
            }
        }
        let addressed = sel.ids.is_some() || sel.queue_keys.is_some() || sel.table_keys.is_some();
        if !addressed {
            match &sel.filter {
                None => return invalid("select rows with where, ids or keys"),
                Some(p) if !self.has_access(p) => {
                    return invalid(
                        "where needs a condition on an indexed column (API-008 §Index rule)",
                    );
                }
                _ => {}
            }
        }
        Ok(sel)
    }

    /// True if `p` has an access term: equality, `in` or `contains` between
    /// an indexed column and a value (API-008 §Index rule). Ranges don't
    /// count: a range alone can match the whole object.
    fn has_access(&self, p: &RPred) -> bool {
        let on_index = |o: &Operand| matches!(o, Operand::Column(c) if self.indexed(c));
        let value = |o: &Operand| !matches!(o, Operand::Column(_) | Operand::Path(..));
        match p {
            RPred::And(ps) => ps.iter().any(|p| self.has_access(p)),
            RPred::Cmp(CmpOp::Eq, a, b) => (on_index(a) && value(b)) || (on_index(b) && value(a)),
            RPred::In(a, vs) => on_index(a) && vs.iter().all(value),
            RPred::Contains(a, v) => on_index(a) && value(v),
            _ => false,
        }
    }
}

impl Ctx<'_> {
    /// True if one btree index holds every column in `cols`, so an aggregate
    /// over them reads only that index (API-008 §Index rule). A queue's
    /// `state` is computed from `state`, `due_at` and `attempts`.
    fn covered(&self, cols: &[&Column]) -> bool {
        let mut need = BTreeSet::new();
        for c in cols {
            match (self.obj.kind, c.system) {
                (Kind::Queue, true) if c.name == "state" => {
                    need.extend(["state", "due_at", "attempts"].map(String::from))
                }
                (_, true) => {
                    need.insert(c.name.clone());
                }
                (_, false) => {
                    need.insert(format!("f_{}", c.name));
                }
            }
        }
        self.btree_indexes()
            .iter()
            .any(|ix| need.iter().all(|c| ix.contains(c)))
    }

    /// The columns of each full btree index on the object (API-008
    /// §PostgreSQL executor). An expiring table has none that cover its
    /// rows, because every read also tests `expires_at`.
    fn btree_indexes(&self) -> Vec<Vec<String>> {
        let mut out = vec![vec!["id".to_string()]];
        let skip = match (self.queue, self.table) {
            (Some(_), _) => {
                out.push(vec!["key".into()]);
                out.push(["state", "due_at", "attempts"].map(String::from).to_vec());
                None
            }
            (_, Some(t)) => {
                if t.expire_after_write.is_some() {
                    return vec![];
                }
                if !t.key.is_empty() {
                    out.push(t.key.iter().map(|k| format!("f_{k}")).collect());
                }
                t.key.first()
            }
            _ => unreachable!(),
        };
        for (f, d) in self.declared() {
            if d.index && d.ty != FieldType::TextSet && Some(f) != skip {
                out.push(vec![format!("f_{f}"), "id".into()]);
            }
        }
        out
    }
}

/// Adds the columns `p` reads to `out`. False if it reads something no index
/// can hold: a body path or another object.
fn pred_columns<'p>(p: &'p RPred, out: &mut Vec<&'p Column>) -> bool {
    fn op<'p>(o: &'p Operand, out: &mut Vec<&'p Column>) -> bool {
        match o {
            Operand::Column(c) => {
                out.push(c);
                true
            }
            Operand::Path(..) => false,
            Operand::Literal(..) | Operand::Now(_) => true,
        }
    }
    match p {
        RPred::And(ps) | RPred::Or(ps) => ps.iter().all(|p| pred_columns(p, out)),
        RPred::Not(b) => pred_columns(b, out),
        RPred::Cmp(_, a, b) | RPred::Contains(a, b) => op(a, out) && op(b, out),
        RPred::In(a, vs) => op(a, out) && vs.iter().all(|v| op(v, out)),
        RPred::IsNull(a) | RPred::NotNull(a) => op(a, out),
        RPred::Exists { .. } => false,
    }
}

/// True for a claim filter made only of `not exists` lookups: it skips the
/// few items it excludes, so it needs no access term of its own.
fn exclusion_only(p: &RPred) -> bool {
    match p {
        RPred::Not(b) => matches!(**b, RPred::Exists { .. }),
        RPred::And(ps) => !ps.is_empty() && ps.iter().all(exclusion_only),
        _ => false,
    }
}

fn two(a: &[Value]) -> Result<[&Value; 2]> {
    match a {
        [x, y] => Ok([x, y]),
        _ => invalid("expected two operands"),
    }
}

fn path(v: &Value) -> Result<Vec<String>> {
    let s = v
        .as_str()
        .ok_or_else(|| Error::Invalid("a path is a string like /a/b".into()))?;
    let Some(rest) = s.strip_prefix('/') else {
        return invalid(format!("path {s:?} starts with /"));
    };
    let parts: Vec<String> = rest
        .split('/')
        .map(|p| p.replace("~1", "/").replace("~0", "~"))
        .collect();
    if parts.iter().any(String::is_empty) {
        return invalid(format!("path {s:?}"));
    }
    Ok(parts)
}

/// Checks a literal against a type. Null fits every type.
fn literal(v: &Value, ty: FieldType) -> Result<()> {
    let ok = match (ty, v) {
        (_, Value::Null) | (FieldType::Json, _) => true,
        (FieldType::Text, Value::String(s)) | (FieldType::Timestamp, Value::String(s)) => {
            !s.contains('\0')
        }
        (FieldType::Int, Value::Number(n)) => n.is_i64(),
        (FieldType::Float, Value::Number(n)) => n.as_f64().is_some_and(f64::is_finite),
        (FieldType::Bool, Value::Bool(_)) => true,
        (FieldType::TextSet, Value::Array(a)) => a
            .iter()
            .all(|x| x.as_str().is_some_and(|s| !s.contains('\0'))),
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        invalid(format!("{v} is not a {ty:?}"))
    }
}

fn limit(n: Option<u32>, default: u32, max: u32) -> Result<u32> {
    let n = n.unwrap_or(default);
    if n == 0 || n > max {
        return invalid(format!("limit is 1 to {max}"));
    }
    Ok(n)
}

fn statement(s: &Schema, st: &Statement, params: &Params) -> Result<RStatement> {
    Ok(match st {
        Statement::Insert(i) => {
            let c = ctx(s, &i.into, params)?;
            if i.rows.is_empty() || i.rows.len() > MAX_ROWS {
                return invalid("1 to 1000 rows");
            }
            match c.obj.kind {
                Kind::Queue => {
                    if i.on_conflict == OnConflict::Update {
                        return invalid("on_conflict: update is for tables");
                    }
                    let rows: Vec<QueueRow> = i
                        .rows
                        .iter()
                        .map(|r| queue_row(&c, r))
                        .collect::<Result<_>>()?;
                    let mut seen = BTreeSet::new();
                    if rows
                        .iter()
                        .filter_map(|r| r.key.as_ref())
                        .any(|k| !seen.insert(k))
                    {
                        return invalid("a key appears twice in one insert");
                    }
                    RStatement::InsertQueue {
                        obj: c.obj,
                        rows,
                        on_conflict: i.on_conflict,
                    }
                }
                Kind::Table => {
                    let t = c.table.expect("table");
                    let rows = i
                        .rows
                        .iter()
                        .map(|r| {
                            let mut out = BTreeMap::new();
                            for (k, v) in r {
                                let col = c.column(k)?;
                                if col.system {
                                    return invalid(format!("{k} is a system column"));
                                }
                                match c.operand(v, col.ty)? {
                                    Operand::Literal(v, _) => out.insert(k.clone(), v),
                                    _ => return invalid("insert values are literals or params"),
                                };
                            }
                            if let Some(k) = t
                                .key
                                .iter()
                                .find(|k| out.get(*k).is_none_or(Value::is_null))
                            {
                                return invalid(format!("key field {k} is required"));
                            }
                            Ok(out)
                        })
                        .collect::<Result<_>>()?;
                    if t.sequenced && i.on_conflict != OnConflict::Skip {
                        return invalid("a sequenced table has no conflicts");
                    }
                    let rows: Vec<BTreeMap<String, Value>> = rows;
                    let mut seen = BTreeSet::new();
                    if !t.sequenced
                        && rows.iter().any(|r| {
                            !seen.insert(t.key.iter().map(|k| r[k].to_string()).collect::<Vec<_>>())
                        })
                    {
                        return invalid("a key appears twice in one insert");
                    }
                    if i.on_conflict == OnConflict::Update
                        && rows.windows(2).any(|w| !w[0].keys().eq(w[1].keys()))
                    {
                        return invalid(
                            "with on_conflict: update, every row gives the same fields",
                        );
                    }
                    RStatement::InsertTable {
                        obj: c.obj,
                        rows,
                        on_conflict: i.on_conflict,
                    }
                }
            }
        }
        Statement::Claim(cl) => {
            let c = ctx(s, &cl.from, params)?;
            c.queue()?;
            if cl.lease.0 <= 0 {
                return invalid("a lease is longer than 0");
            }
            let eligible_at = cl
                .eligible_at
                .as_ref()
                .map(|v| c.operand(v, FieldType::Timestamp))
                .transpose()?;
            RStatement::Claim {
                limit: limit(Some(cl.limit), 1, MAX_CLAIM)?,
                lease_ms: cl.lease.0,
                worker: cl.worker.clone(),
                filter: match &cl.filter {
                    None => None,
                    Some(p) => {
                        let p = c.pred(p)?;
                        // A claim filter must narrow through an index too, or
                        // a rare value walks the whole claim index.
                        if !c.has_access(&p) && !exclusion_only(&p) {
                            return invalid(
                                "a claim's where needs an access term (API-008 §Index rule)",
                            );
                        }
                        Some(p)
                    }
                },
                eligible_at,
                obj: c.obj,
            }
        }
        Statement::Settle(x) => {
            let c = ctx(s, &x.queue, params)?;
            c.queue()?;
            if x.after.is_some() && !matches!(x.outcome, Outcome::Retry | Outcome::Release) {
                return invalid("after is for retry and release");
            }
            RStatement::Settle {
                items: c.items(&x.items)?,
                after_ms: x.after.map_or(0, |d| d.0),
                set: c.assignments(&x.set, false)?,
                body: c.patches(&x.body)?,
                require_all: x.require_all,
                outcome: x.outcome,
                obj: c.obj,
            }
        }
        Statement::Renew(r) => {
            let c = ctx(s, &r.queue, params)?;
            c.queue()?;
            if r.lease.0 <= 0 {
                return invalid("a lease is longer than 0");
            }
            RStatement::Renew {
                items: c.items(&r.items)?,
                lease_ms: r.lease.0,
                require_all: r.require_all,
                obj: c.obj,
            }
        }
        Statement::Update(u) => {
            let c = ctx(s, &u.target, params)?;
            if c.table.is_some_and(|t| t.sequenced) {
                return invalid("rows of a sequenced table are never updated");
            }
            if c.obj.kind == Kind::Table && u.leased != LeasedPolicy::Skip {
                return invalid("leased is for queues");
            }
            if u.set.is_empty() && u.body.is_empty() {
                return invalid("an update sets fields or patches the body");
            }
            if u.on_limit == OnLimit::Step && u.expect.is_some() {
                return invalid("a stepped update has no expect");
            }
            let schedule = c.obj.kind == Kind::Queue && u.leased != LeasedPolicy::Preserve;
            RStatement::Update {
                sel: c.selection(&u.filter, &u.ids, &u.keys)?,
                set: c.assignments(&u.set, schedule)?,
                body: c.patches(&u.body)?,
                leased: u.leased,
                limit: limit(u.limit, DEFAULT_UPDATE_LIMIT, MAX_UPDATE)?,
                on_limit: u.on_limit,
                expect: u.expect,
                override_merge: u.override_merge,
                obj: c.obj,
            }
        }
        Statement::Delete(d) => {
            let c = ctx(s, &d.target, params)?;
            RStatement::Delete {
                sel: c.selection(&d.filter, &d.ids, &d.keys)?,
                limit: limit(d.limit, DEFAULT_UPDATE_LIMIT, MAX_UPDATE)?,
                expect: d.expect,
                obj: c.obj,
            }
        }
        Statement::Assert(a) => {
            let c = ctx(s, &a.target, params)?;
            if a.expect.min.is_none() && a.expect.max.is_none() {
                return invalid("expect needs min or max");
            }
            RStatement::Assert {
                sel: c.selection(&a.filter, &a.ids, &a.keys)?,
                expect: a.expect,
                obj: c.obj,
            }
        }
        Statement::Select(sel) => RStatement::Select(resolve_select(s, sel, params)?),
        Statement::Read(r) => {
            let decl = s
                .reads
                .iter()
                .find(|d| d.name == r.name && d.version == r.version)
                .ok_or_else(|| {
                    Error::Invalid(format!("no read {} version {}", r.name, r.version))
                })?;
            for (k, v) in &r.params {
                let t = decl
                    .params
                    .get(k)
                    .ok_or_else(|| Error::Invalid(format!("read {}: no param {k}", r.name)))?;
                literal(v, *t)?;
            }
            if let Some(k) = decl.params.keys().find(|k| !r.params.contains_key(*k)) {
                return invalid(format!("read {}: param {k} missing", r.name));
            }
            RStatement::Select(resolve_select(s, &decl.select, &r.params)?)
        }
        Statement::Pause(q) | Statement::Resume(q) => {
            let c = ctx(s, &q.queue, params)?;
            c.queue()?;
            RStatement::SetPaused {
                obj: c.obj,
                paused: matches!(st, Statement::Pause(_)),
            }
        }
    })
}

fn queue_row(c: &Ctx<'_>, r: &BTreeMap<String, Value>) -> Result<QueueRow> {
    let mut row = QueueRow::default();
    let lit = |v: &Value, t| match c.operand(v, t)? {
        Operand::Literal(v, _) => Ok(v),
        _ => invalid("insert values are literals or params"),
    };
    for (k, v) in r {
        match k.as_str() {
            "key" => row.key = lit(v, FieldType::Text)?.as_str().map(str::to_owned),
            "priority" => row.priority = lit(v, FieldType::Int)?.as_i64().unwrap_or(0),
            "due" => row.due = lit(v, FieldType::Timestamp)?.as_str().map(str::to_owned),
            "after" => {
                let d: Duration =
                    serde_json::from_value(v.clone()).map_err(|e| Error::Invalid(e.to_string()))?;
                row.after_ms = d.0;
            }
            "body" => row.body = Some(lit(v, FieldType::Json)?),
            _ => {
                let col = c.column(k)?;
                if col.system {
                    return invalid(format!("{k} can't be set on insert"));
                }
                row.fields.insert(k.clone(), lit(v, col.ty)?);
            }
        }
    }
    if row.due.is_some() && row.after_ms != 0 {
        return invalid("give due or after, not both");
    }
    Ok(row)
}

/// Resolves a `select` (also used for declared reads).
pub fn resolve_select(s: &Schema, sel: &Select, params: &Params) -> Result<RSelect> {
    let c = ctx(s, &sel.from, params)?;
    let filter = sel.filter.as_ref().map(|p| c.pred(p)).transpose()?;
    let output = match (sel.fields.is_empty(), sel.aggregate.is_empty()) {
        (false, true) => {
            if !sel.group_by.is_empty() {
                return invalid("group_by goes with aggregate");
            }
            Output::Fields(
                sel.fields
                    .iter()
                    .map(|f| c.column(f))
                    .collect::<Result<_>>()?,
            )
        }
        (true, false) => {
            let mut aggs = vec![];
            for (name, a) in &sel.aggregate {
                if !valid_name(name) {
                    return invalid(format!("aggregate name {name:?}"));
                }
                aggs.push((name.clone(), agg(&c, a)?));
            }
            Output::Aggregate {
                aggs,
                group_by: sel
                    .group_by
                    .iter()
                    .map(|f| c.column(f))
                    .collect::<Result<_>>()?,
            }
        }
        _ => return invalid("select has fields or aggregate"),
    };
    let names: Vec<String> = match &output {
        Output::Fields(cols) => cols.iter().map(|c| c.name.clone()).collect(),
        Output::Aggregate { aggs, group_by } => group_by
            .iter()
            .map(|c| c.name.clone())
            .chain(aggs.iter().map(|(n, _)| n.clone()))
            .collect(),
    };
    let order_by = sel
        .order_by
        .iter()
        .map(|o| {
            if names.contains(&o.field) {
                Ok((o.field.clone(), o.desc))
            } else {
                invalid(format!("order_by {} is not in the output", o.field))
            }
        })
        .collect::<Result<_>>()?;
    // An access term bounds the rows read. Without one, an aggregate may
    // still run if a single index holds every column it reads.
    let access = filter.as_ref().is_some_and(|p| c.has_access(p));
    if !access {
        let mut cols = vec![];
        let readable = filter.as_ref().is_none_or(|p| pred_columns(p, &mut cols));
        let covered = match &output {
            Output::Aggregate { aggs, group_by } if readable => {
                cols.extend(group_by);
                cols.extend(aggs.iter().filter_map(|(_, a)| match a {
                    Agg::Count => None,
                    Agg::Sum(c) | Agg::Min(c) | Agg::Max(c) | Agg::Avg(c) => Some(c),
                    Agg::Percentile(c, _) => Some(c),
                }));
                c.covered(&cols)
            }
            _ => false,
        };
        if !covered {
            return invalid(
                "where needs a condition on an indexed column, or the aggregate must read only \
                 the columns of one index (API-008 §Index rule)",
            );
        }
    }
    Ok(RSelect {
        obj: c.obj.clone(),
        filter,
        output,
        order_by,
        limit: limit(Some(sel.limit), 1, MAX_SELECT)?,
    })
}

fn agg(c: &Ctx<'_>, a: &Value) -> Result<Agg> {
    if a.as_str() == Some("count") {
        return Ok(Agg::Count);
    }
    let Value::Object(m) = a else {
        return invalid(format!("aggregate {a}"));
    };
    let (k, arg) = match m.iter().next() {
        Some(x) if m.len() == 1 => x,
        _ => return invalid(format!("aggregate {a}")),
    };
    let numeric = |name: &Value| -> Result<Column> {
        let col = c.column(name.as_str().unwrap_or(""))?;
        if !matches!(
            col.ty,
            FieldType::Int | FieldType::Float | FieldType::Timestamp
        ) {
            return invalid(format!("{k} needs a number or time"));
        }
        Ok(col)
    };
    Ok(match k.as_str() {
        "sum" | "avg" => {
            let col = numeric(arg)?;
            if col.ty == FieldType::Timestamp {
                return invalid(format!("{k} needs a number"));
            }
            if k == "sum" {
                Agg::Sum(col)
            } else {
                Agg::Avg(col)
            }
        }
        "min" => Agg::Min(numeric(arg)?),
        "max" => Agg::Max(numeric(arg)?),
        "percentile" => {
            let [f, p] = two(arg.as_array().map(Vec::as_slice).unwrap_or(&[]))?;
            let p = p
                .as_f64()
                .filter(|p| (0.0..=1.0).contains(p))
                .ok_or_else(|| Error::Invalid("percentile is 0 to 1".into()))?;
            Agg::Percentile(numeric(f)?, p)
        }
        _ => return invalid(format!("aggregate {k}")),
    })
}
