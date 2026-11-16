//! The expression language: typing, evaluation, casts, aggregates and the
//! index rule (API-006 §Expressions). This module is the reference
//! semantics; every implementation must produce the same results.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{FieldName, JsonPointer, ParamName, UtcMillis};
use crate::value::{
    ScalarType, Value, check_domain, float_in_domain, format_rfc3339, parse_rfc3339,
    timestamp_in_domain, value_to_json,
};

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Lit(Value),
    Field(FieldName),
    Param(ParamName),
    Now,
    Json {
        field: FieldName,
        pointer: JsonPointer,
    },
    Cast {
        expr: Box<Expr>,
        to: ScalarType,
    },
    Cmp {
        op: CmpOp,
        l: Box<Expr>,
        r: Box<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    Contains {
        set: Box<Expr>,
        value: Box<Expr>,
    },
    SetAdd {
        set: Box<Expr>,
        values: Vec<Expr>,
    },
    SetRemove {
        set: Box<Expr>,
        values: Vec<Expr>,
    },
    Arith {
        op: ArithOp,
        l: Box<Expr>,
        r: Box<Expr>,
    },
    Coalesce(Vec<Expr>),
    Bucket {
        expr: Box<Expr>,
        width_ms: u64,
    },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    Count,
    CountOf(Expr),
    Sum(Expr),
    Min(Expr),
    Max(Expr),
    Avg(Expr),
    /// `p_millionths` in `1 ..= 1_000_000`.
    Percentile {
        expr: Expr,
        p_millionths: u32,
    },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Asc,
    Desc,
}

/// A fault (API-006 §Evaluation rule 3).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvalError {
    IntOverflow,
    DivideByZero,
    FloatOutOfRange,
    TimestampOutOfRange,
    SetTooLarge,
    PointerConflict,
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Smart constructors.
impl Expr {
    pub fn lit(v: Value) -> Self {
        Expr::Lit(v)
    }
    pub fn int(i: i64) -> Self {
        Expr::Lit(Value::Int(i))
    }
    pub fn float(f: f64) -> Self {
        Expr::Lit(Value::Float(f))
    }
    pub fn text(s: &str) -> Self {
        Expr::Lit(Value::Text(s.to_owned()))
    }
    pub fn boolean(b: bool) -> Self {
        Expr::Lit(Value::Bool(b))
    }
    pub fn null(t: ScalarType) -> Self {
        Expr::Lit(Value::Null(t))
    }
    pub fn field(name: &str) -> Self {
        Expr::Field(FieldName::new(name))
    }
    pub fn param(name: &str) -> Self {
        Expr::Param(ParamName::new(name))
    }
    pub fn cmp(op: CmpOp, l: Expr, r: Expr) -> Self {
        Expr::Cmp {
            op,
            l: Box::new(l),
            r: Box::new(r),
        }
    }
    pub fn eq(l: Expr, r: Expr) -> Self {
        Self::cmp(CmpOp::Eq, l, r)
    }
    pub fn arith(op: ArithOp, l: Expr, r: Expr) -> Self {
        Expr::Arith {
            op,
            l: Box::new(l),
            r: Box::new(r),
        }
    }
    pub fn cast(e: Expr, to: ScalarType) -> Self {
        Expr::Cast {
            expr: Box::new(e),
            to,
        }
    }
}

// ------------------------------------------------------------------ typing

/// Type information an expression is checked against.
pub trait TypeEnv {
    fn field_type(&self, f: &FieldName) -> Option<ScalarType>;
    fn param_type(&self, p: &ParamName) -> Option<ScalarType>;
}

/// A `TypeEnv` from maps.
#[derive(Default)]
pub struct MapTypeEnv {
    pub fields: BTreeMap<FieldName, ScalarType>,
    pub params: BTreeMap<ParamName, ScalarType>,
}

impl TypeEnv for MapTypeEnv {
    fn field_type(&self, f: &FieldName) -> Option<ScalarType> {
        self.fields.get(f).copied()
    }
    fn param_type(&self, p: &ParamName) -> Option<ScalarType> {
        self.params.get(p).copied()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeError(pub String);

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn terr<T>(msg: impl Into<String>) -> Result<T, TypeError> {
    Err(TypeError(msg.into()))
}

pub const MAX_BOOL_OPERANDS: usize = 64;
pub const MAX_COALESCE_OPERANDS: usize = 16;
pub const MAX_BUCKET_WIDTH_MS: u64 = 315_576_000_000;

fn eq_comparable(t: ScalarType) -> bool {
    use ScalarType::*;
    matches!(t, Bool | Int | Float | Text | Bytes | Timestamp)
}

fn orderable(t: ScalarType) -> bool {
    use ScalarType::*;
    matches!(t, Int | Float | Text | Bytes | Timestamp)
}

/// True if `from` may be cast to `to` (API-006 §Casts).
pub fn cast_allowed(from: ScalarType, to: ScalarType) -> bool {
    use ScalarType::*;
    if from == to {
        return true;
    }
    matches!(
        (from, to),
        (Bool, Text | Json)
            | (Int, Float | Text | Timestamp | Json)
            | (Float, Int | Text | Json)
            | (Text, Bool | Int | Float | Timestamp | Json)
            | (Timestamp, Int | Text | Json)
            | (Json, Bool | Int | Float | Text | Timestamp)
            | (Bytes | TextSet, Json)
    )
}

/// The static type of an expression (API-006 §Typing).
pub fn type_of(e: &Expr, env: &dyn TypeEnv) -> Result<ScalarType, TypeError> {
    use ScalarType::*;
    match e {
        Expr::Lit(v) => Ok(v.ty()),
        Expr::Field(f) => env
            .field_type(f)
            .ok_or_else(|| TypeError(format!("unknown field {f}"))),
        Expr::Param(p) => env
            .param_type(p)
            .ok_or_else(|| TypeError(format!("missing parameter {p}"))),
        Expr::Now => Ok(Timestamp),
        Expr::Json { field, .. } => match env.field_type(field) {
            Some(Json) => Ok(Json),
            Some(t) => terr(format!("Json access on {field} of type {t:?}")),
            None => terr(format!("unknown field {field}")),
        },
        Expr::Cast { expr, to } => {
            let from = type_of(expr, env)?;
            if cast_allowed(from, *to) {
                Ok(*to)
            } else {
                terr(format!("cannot cast {from:?} to {to:?}"))
            }
        }
        Expr::Cmp { op, l, r } => {
            let (lt, rt) = (type_of(l, env)?, type_of(r, env)?);
            if lt != rt {
                return terr(format!("comparison of {lt:?} with {rt:?}"));
            }
            let ok = match op {
                CmpOp::Eq | CmpOp::Ne => eq_comparable(lt),
                _ => orderable(lt),
            };
            if ok {
                Ok(Bool)
            } else {
                terr(format!("{op:?} not defined on {lt:?}"))
            }
        }
        Expr::And(xs) | Expr::Or(xs) => {
            if xs.is_empty() || xs.len() > MAX_BOOL_OPERANDS {
                return terr("And/Or takes 1 to 64 operands");
            }
            for x in xs {
                if type_of(x, env)? != Bool {
                    return terr("And/Or operand is not Bool");
                }
            }
            Ok(Bool)
        }
        Expr::Not(x) => {
            if type_of(x, env)? != Bool {
                return terr("Not operand is not Bool");
            }
            Ok(Bool)
        }
        Expr::IsNull(x) => {
            type_of(x, env)?;
            Ok(Bool)
        }
        Expr::In { expr, list } => {
            let t = type_of(expr, env)?;
            if !eq_comparable(t) {
                return terr(format!("In not defined on {t:?}"));
            }
            if list.is_empty() {
                return terr("In list is empty");
            }
            for x in list {
                if type_of(x, env)? != t {
                    return terr("In list item type differs");
                }
            }
            Ok(Bool)
        }
        Expr::Contains { set, value } => match (type_of(set, env)?, type_of(value, env)?) {
            (TextSet, Text) => Ok(Bool),
            (a, b) => terr(format!("Contains on {a:?} with {b:?}")),
        },
        Expr::SetAdd { set, values } | Expr::SetRemove { set, values } => {
            let st = type_of(set, env)?;
            let et = match st {
                TextSet => Text,
                _ => return terr(format!("set operation on {st:?}")),
            };
            for v in values {
                if type_of(v, env)? != et {
                    return terr("set element type differs");
                }
            }
            Ok(st)
        }
        Expr::Arith { op, l, r } => {
            let (lt, rt) = (type_of(l, env)?, type_of(r, env)?);
            match (op, lt, rt) {
                (ArithOp::Rem, Int, Int) => Ok(Int),
                (ArithOp::Rem, _, _) => terr("Rem needs Int operands"),
                (_, Int, Int) => Ok(Int),
                (_, Float, Float) => Ok(Float),
                (ArithOp::Add, Timestamp, Int) => Ok(Timestamp),
                (ArithOp::Sub, Timestamp, Int) => Ok(Timestamp),
                (ArithOp::Sub, Timestamp, Timestamp) => Ok(Int),
                _ => terr(format!("{op:?} not defined on {lt:?} and {rt:?}")),
            }
        }
        Expr::Coalesce(xs) => {
            if xs.is_empty() || xs.len() > MAX_COALESCE_OPERANDS {
                return terr("Coalesce takes 1 to 16 operands");
            }
            let t = type_of(&xs[0], env)?;
            for x in &xs[1..] {
                if type_of(x, env)? != t {
                    return terr("Coalesce operand types differ");
                }
            }
            Ok(t)
        }
        Expr::Bucket { expr, width_ms } => {
            if *width_ms == 0 || *width_ms > MAX_BUCKET_WIDTH_MS {
                return terr("Bucket width outside 1 ms to 10 years");
            }
            if type_of(expr, env)? != Timestamp {
                return terr("Bucket needs a Timestamp");
            }
            Ok(Timestamp)
        }
    }
}

/// The result type of an aggregate over an operand of type `operand`
/// (`None` for `Count`).
pub fn aggregate_type(a: &Aggregate, env: &dyn TypeEnv) -> Result<ScalarType, TypeError> {
    use ScalarType::*;
    match a {
        Aggregate::Count => Ok(Int),
        Aggregate::CountOf(e) => type_of(e, env).map(|_| Int),
        Aggregate::Sum(e) => match type_of(e, env)? {
            Int => Ok(Int),
            Float => Ok(Float),
            t => terr(format!("Sum on {t:?}")),
        },
        Aggregate::Min(e) | Aggregate::Max(e) => {
            let t = type_of(e, env)?;
            if orderable(t) {
                Ok(t)
            } else {
                terr(format!("Min/Max on {t:?}"))
            }
        }
        Aggregate::Avg(e) => match type_of(e, env)? {
            Int | Float => Ok(Float),
            t => terr(format!("Avg on {t:?}")),
        },
        Aggregate::Percentile { expr, p_millionths } => {
            if !(1..=1_000_000).contains(p_millionths) {
                return terr("Percentile p outside 1 to 1,000,000");
            }
            match type_of(expr, env)? {
                t @ (Int | Float | Timestamp) => Ok(t),
                t => terr(format!("Percentile on {t:?}")),
            }
        }
    }
}

/// Types allowed in `group_by`.
pub fn groupable(t: ScalarType) -> bool {
    use ScalarType::*;
    matches!(t, Bool | Int | Text | Bytes | Timestamp)
}

/// Types allowed in `order`.
pub fn sortable(t: ScalarType) -> bool {
    orderable(t) || t == ScalarType::Bool
}

// -------------------------------------------------------------- evaluation

/// Field values of the record an expression is evaluated against.
pub trait RowAccess {
    /// The field's value, or `None` if the record has no such field.
    fn field(&self, f: &FieldName) -> Option<Value>;
}

impl RowAccess for BTreeMap<FieldName, Value> {
    fn field(&self, f: &FieldName) -> Option<Value> {
        self.get(f).cloned()
    }
}

/// A record with no fields.
pub struct NoRow;

impl RowAccess for NoRow {
    fn field(&self, _: &FieldName) -> Option<Value> {
        None
    }
}

/// Where a fault occurs (API-006 §Evaluation rule 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultMode {
    /// Predicates and read outputs: the faulting expression yields null.
    Null,
    /// Action positions: the fault is returned.
    Error,
}

pub struct EvalCtx<'a> {
    pub row: &'a dyn RowAccess,
    pub params: &'a BTreeMap<ParamName, Value>,
    pub now: UtcMillis,
    pub max_set_values: u32,
    pub mode: FaultMode,
}

impl EvalCtx<'_> {
    fn fault(&self, e: EvalError, ty: ScalarType) -> Result<Value, EvalError> {
        match self.mode {
            FaultMode::Null => Ok(Value::Null(ty)),
            FaultMode::Error => Err(e),
        }
    }
}

/// Evaluates a type-checked expression.
pub fn eval(e: &Expr, cx: &EvalCtx<'_>) -> Result<Value, EvalError> {
    use ScalarType::*;
    match e {
        Expr::Lit(v) => Ok(v.clone()),
        Expr::Field(f) => Ok(cx.row.field(f).unwrap_or(Value::Null(Json))),
        Expr::Param(p) => Ok(cx.params.get(p).cloned().unwrap_or(Value::Null(Json))),
        Expr::Now => Ok(Value::Timestamp(cx.now)),
        Expr::Json { field, pointer } => match cx.row.field(field) {
            Some(Value::Json(j)) => Ok(match j.pointer(pointer.as_str()) {
                Some(x) => Value::Json(x.clone()),
                None => Value::Null(Json),
            }),
            _ => Ok(Value::Null(Json)),
        },
        Expr::Cast { expr, to } => Ok(cast(&eval(expr, cx)?, *to)),
        Expr::Cmp { op, l, r } => {
            let (a, b) = (eval(l, cx)?, eval(r, cx)?);
            if a.is_null() || b.is_null() {
                return Ok(Value::Null(Bool));
            }
            let ord = compare(&a, &b);
            Ok(Value::Bool(match op {
                CmpOp::Eq => ord == Some(Ordering::Equal),
                CmpOp::Ne => ord != Some(Ordering::Equal),
                CmpOp::Lt => ord == Some(Ordering::Less),
                CmpOp::Le => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
                CmpOp::Gt => ord == Some(Ordering::Greater),
                CmpOp::Ge => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
            }))
        }
        Expr::And(xs) => {
            let mut saw_null = false;
            for x in xs {
                match eval(x, cx)? {
                    Value::Bool(false) => return Ok(Value::Bool(false)),
                    Value::Bool(true) => {}
                    _ => saw_null = true,
                }
            }
            Ok(if saw_null {
                Value::Null(Bool)
            } else {
                Value::Bool(true)
            })
        }
        Expr::Or(xs) => {
            let mut saw_null = false;
            for x in xs {
                match eval(x, cx)? {
                    Value::Bool(true) => return Ok(Value::Bool(true)),
                    Value::Bool(false) => {}
                    _ => saw_null = true,
                }
            }
            Ok(if saw_null {
                Value::Null(Bool)
            } else {
                Value::Bool(false)
            })
        }
        Expr::Not(x) => Ok(match eval(x, cx)? {
            Value::Bool(b) => Value::Bool(!b),
            _ => Value::Null(Bool),
        }),
        Expr::IsNull(x) => Ok(Value::Bool(eval(x, cx)?.is_null())),
        Expr::In { expr, list } => {
            // `In` does not short-circuit: every operand is evaluated, so a
            // fault anywhere in the list is observed (API-006 §Evaluation
            // rule 1).
            let v = eval(expr, cx)?;
            let mut items = Vec::with_capacity(list.len());
            for x in list {
                items.push(eval(x, cx)?);
            }
            let mut saw_null = v.is_null();
            let mut found = false;
            for item in &items {
                if item.is_null() {
                    saw_null = true;
                } else if !v.is_null() && compare(&v, item) == Some(Ordering::Equal) {
                    found = true;
                }
            }
            Ok(if found {
                Value::Bool(true)
            } else if saw_null {
                Value::Null(Bool)
            } else {
                Value::Bool(false)
            })
        }
        Expr::Contains { set, value } => {
            let (s, v) = (eval(set, cx)?, eval(value, cx)?);
            Ok(match (s, v) {
                (Value::TextSet(s), Value::Text(t)) => Value::Bool(s.contains(&t)),
                _ => Value::Null(Bool),
            })
        }
        Expr::SetAdd { set, values } | Expr::SetRemove { set, values } => {
            let add = matches!(e, Expr::SetAdd { .. });
            let base = eval(set, cx)?;
            let mut vals = Vec::with_capacity(values.len());
            for v in values {
                vals.push(eval(v, cx)?);
            }
            let out = match base {
                Value::TextSet(_) | Value::Null(TextSet) => {
                    let mut s = match base {
                        Value::TextSet(s) => s,
                        _ => BTreeSet::new(),
                    };
                    for v in vals {
                        if let Value::Text(t) = v {
                            if add {
                                s.insert(t);
                            } else {
                                s.remove(&t);
                            }
                        }
                    }
                    Value::TextSet(s)
                }
                other => other,
            };
            let len = match &out {
                Value::TextSet(s) => s.len(),
                _ => 0,
            };
            if len > cx.max_set_values as usize {
                return cx.fault(EvalError::SetTooLarge, out.ty());
            }
            Ok(out)
        }
        Expr::Arith { op, l, r } => {
            let (a, b) = (eval(l, cx)?, eval(r, cx)?);
            arith(*op, &a, &b, cx)
        }
        Expr::Coalesce(xs) => {
            let mut last = Value::Null(Json);
            for x in xs {
                let v = eval(x, cx)?;
                if !v.is_null() {
                    return Ok(v);
                }
                last = v;
            }
            Ok(last)
        }
        Expr::Bucket { expr, width_ms } => match eval(expr, cx)? {
            Value::Timestamp(t) => {
                let w = *width_ms as i64;
                let b = t.0.div_euclid(w) * w;
                if timestamp_in_domain(b) {
                    Ok(Value::Timestamp(UtcMillis(b)))
                } else {
                    cx.fault(EvalError::TimestampOutOfRange, Timestamp)
                }
            }
            _ => Ok(Value::Null(Timestamp)),
        },
    }
}

fn arith(op: ArithOp, a: &Value, b: &Value, cx: &EvalCtx<'_>) -> Result<Value, EvalError> {
    use ScalarType::*;
    let result_ty = match (a.ty(), b.ty()) {
        (Int, Int) => Int,
        (Float, Float) => Float,
        (Timestamp, Int) => Timestamp,
        (Timestamp, Timestamp) => Int,
        (t, _) => t,
    };
    if a.is_null() || b.is_null() {
        return Ok(Value::Null(result_ty));
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            let r = match op {
                ArithOp::Add => x.checked_add(*y),
                ArithOp::Sub => x.checked_sub(*y),
                ArithOp::Mul => x.checked_mul(*y),
                ArithOp::Div | ArithOp::Rem if *y == 0 => {
                    return cx.fault(EvalError::DivideByZero, Int);
                }
                ArithOp::Div => x.checked_div(*y),
                ArithOp::Rem => x.checked_rem(*y),
            };
            match r {
                Some(v) => Ok(Value::Int(v)),
                None => cx.fault(EvalError::IntOverflow, Int),
            }
        }
        (Value::Float(x), Value::Float(y)) => {
            let r = match op {
                ArithOp::Add => x + y,
                ArithOp::Sub => x - y,
                ArithOp::Mul => x * y,
                ArithOp::Div => x / y,
                ArithOp::Rem => f64::NAN,
            };
            if float_in_domain(r) {
                Ok(Value::Float(r))
            } else {
                cx.fault(EvalError::FloatOutOfRange, Float)
            }
        }
        (Value::Timestamp(t), Value::Int(d)) => {
            let r = match op {
                ArithOp::Add => t.0.checked_add(*d),
                _ => t.0.checked_sub(*d),
            };
            match r {
                Some(v) if timestamp_in_domain(v) => Ok(Value::Timestamp(UtcMillis(v))),
                _ => cx.fault(EvalError::TimestampOutOfRange, Timestamp),
            }
        }
        (Value::Timestamp(x), Value::Timestamp(y)) => Ok(Value::Int(x.0 - y.0)),
        _ => Ok(Value::Null(result_ty)),
    }
}

/// Total order between two non-null values of one type (`None` if the
/// type is not comparable). Text compares by code point, Bytes bytewise.
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Text(x), Value::Text(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
        (Value::Bytes(x), Value::Bytes(y)) => Some(x.as_ref().cmp(y.as_ref())),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Sort order for reads: null sorts first (API-006 §Typing, reads).
pub fn sort_compare(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => compare(a, b).unwrap_or(Ordering::Equal),
    }
}

/// Evaluates a predicate: faults yield null, and only `true` selects.
pub fn eval_predicate(
    e: &Expr,
    row: &dyn RowAccess,
    params: &BTreeMap<ParamName, Value>,
    now: UtcMillis,
    max_set_values: u32,
) -> bool {
    let cx = EvalCtx {
        row,
        params,
        now,
        max_set_values,
        mode: FaultMode::Null,
    };
    matches!(eval(e, &cx), Ok(Value::Bool(true)))
}

// ------------------------------------------------------------------ casts

fn text_to_int(s: &str) -> Option<i64> {
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn text_to_float(s: &str) -> Option<f64> {
    let ok_chars = s
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'));
    if !ok_chars || !s.bytes().any(|b| b.is_ascii_digit()) {
        return None;
    }
    let f: f64 = s.parse().ok()?;
    float_in_domain(f).then_some(f)
}

/// Casts a value (API-006 §Casts). Never faults: an impossible conversion
/// yields null.
pub fn cast(v: &Value, to: ScalarType) -> Value {
    use ScalarType::*;
    if v.ty() == to {
        return v.clone();
    }
    let null = Value::Null(to);
    match (v, to) {
        (Value::Null(_), _) => null,
        (_, Json) => Value::Json(value_to_json(v)),
        (Value::Bool(b), Text) => Value::Text(b.to_string()),
        (Value::Int(i), Float) => Value::Float(*i as f64),
        (Value::Int(i), Text) => Value::Text(i.to_string()),
        (Value::Int(i), Timestamp) => {
            if timestamp_in_domain(*i) {
                Value::Timestamp(UtcMillis(*i))
            } else {
                null
            }
        }
        (Value::Float(f), Int) => {
            let t = f.trunc();
            if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&t) {
                Value::Int(t as i64)
            } else {
                null
            }
        }
        (Value::Float(f), Text) => Value::Text(format!("{f}")),
        (Value::Text(s), Bool) => match s.as_str() {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => null,
        },
        (Value::Text(s), Int) => text_to_int(s).map(Value::Int).unwrap_or(null),
        (Value::Text(s), Float) => text_to_float(s).map(Value::Float).unwrap_or(null),
        (Value::Text(s), Timestamp) => parse_rfc3339(s)
            .map(|m| Value::Timestamp(UtcMillis(m)))
            .unwrap_or(null),
        (Value::Timestamp(t), Int) => Value::Int(t.0),
        (Value::Timestamp(t), Text) => Value::Text(format_rfc3339(t.0)),
        (Value::Json(j), _) => json_cast(j, to),
        _ => null,
    }
}

fn json_cast(j: &serde_json::Value, to: ScalarType) -> Value {
    use ScalarType::*;
    use serde_json::Value as J;
    let null = Value::Null(to);
    match (j, to) {
        (J::Bool(b), Bool) => Value::Bool(*b),
        (J::Number(n), Int) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                if f.fract() == 0.0
                    && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f)
                {
                    Value::Int(f as i64)
                } else {
                    null
                }
            } else {
                null
            }
        }
        (J::Number(n), Float) => match n.as_f64() {
            Some(f) if float_in_domain(f) => Value::Float(f),
            _ => null,
        },
        (J::String(s), Text) => Value::Text(s.clone()),
        (J::String(s), Timestamp) => parse_rfc3339(s)
            .map(|m| Value::Timestamp(UtcMillis(m)))
            .unwrap_or(null),
        _ => null,
    }
}

// ------------------------------------------------------------- aggregates

/// Computes an aggregate over already-evaluated operand values
/// (API-006 §Aggregates). `rows` is the input row count (for `Count`);
/// `ty` is the aggregate's result type.
pub fn aggregate(
    a: &Aggregate,
    rows: u64,
    values: &[Value],
    ty: ScalarType,
) -> Result<Value, EvalError> {
    let non_null: Vec<&Value> = values.iter().filter(|v| !v.is_null()).collect();
    match a {
        Aggregate::Count => Ok(Value::Int(rows as i64)),
        Aggregate::CountOf(_) => Ok(Value::Int(non_null.len() as i64)),
        _ if non_null.is_empty() => Ok(Value::Null(ty)),
        Aggregate::Sum(_) => match ty {
            ScalarType::Int => {
                let mut acc: i128 = 0;
                for v in &non_null {
                    if let Value::Int(i) = v {
                        acc += *i as i128;
                    }
                }
                i64::try_from(acc)
                    .map(Value::Int)
                    .map_err(|_| EvalError::IntOverflow)
            }
            _ => Ok(Value::Float(
                non_null.iter().filter_map(|v| as_f64(v)).sum(),
            )),
        },
        Aggregate::Avg(_) => {
            let n = non_null.len() as i128;
            if let Value::Int(_) = non_null[0] {
                let mut acc: i128 = 0;
                for v in &non_null {
                    if let Value::Int(i) = v {
                        acc += *i as i128;
                    }
                }
                Ok(Value::Float(exact_quotient_f64(acc, n)))
            } else {
                let s: f64 = non_null.iter().filter_map(|v| as_f64(v)).sum();
                Ok(Value::Float(s / n as f64))
            }
        }
        Aggregate::Min(_) | Aggregate::Max(_) => {
            let min = matches!(a, Aggregate::Min(_));
            let mut best = non_null[0];
            for v in &non_null[1..] {
                let o = compare(v, best).unwrap_or(Ordering::Equal);
                if (min && o == Ordering::Less) || (!min && o == Ordering::Greater) {
                    best = v;
                }
            }
            Ok(best.clone())
        }
        Aggregate::Percentile { p_millionths, .. } => {
            let mut sorted: Vec<&Value> = non_null.clone();
            sorted.sort_by(|x, y| compare(x, y).unwrap_or(Ordering::Equal));
            let rank = percentile_rank(*p_millionths, sorted.len() as u64);
            Ok(sorted[(rank - 1) as usize].clone())
        }
    }
}

/// `num / den` rounded once to the nearest `f64` (ties to even), by
/// writing the exact decimal with a sticky digit and letting Rust's
/// correctly rounded parser round it.
pub fn exact_quotient_f64(num: i128, den: i128) -> f64 {
    let neg = (num < 0) != (den < 0);
    let (n, d) = (num.unsigned_abs(), den.unsigned_abs());
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(&(n / d).to_string());
    s.push('.');
    let mut r = n % d;
    for _ in 0..60 {
        r *= 10;
        s.push(char::from(b'0' + (r / d) as u8));
        r %= d;
    }
    if r != 0 {
        s.push('1');
    }
    s.parse().expect("decimal parses")
}

/// Nearest-rank: `ceil(p × n / 1,000,000)` in integer arithmetic, at least 1.
pub fn percentile_rank(p_millionths: u32, n: u64) -> u64 {
    let r = (p_millionths as u128 * n as u128).div_ceil(1_000_000) as u64;
    r.max(1)
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float(f) => Some(*f),
        Value::Int(i) => Some(*i as f64),
        _ => None,
    }
}

// -------------------------------------------------------------- index rule

/// The index declared on a field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Lookup,
    Range,
    Unique,
    /// Per-value index on a `TextSet`.
    SetMember,
}

/// The access term of a predicate (API-006 §Index rule).
#[derive(Clone, Debug, PartialEq)]
pub struct AccessTerm {
    pub field: FieldName,
    /// Index of the conjunct (or the first of two range conjuncts).
    pub conjunct: usize,
    pub kind: AccessKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AccessKind {
    /// `Eq` or `In`.
    Equality,
    /// One or two range bounds.
    Range,
    /// `Contains` on an indexed set.
    Member,
}

fn literalish(e: &Expr) -> bool {
    matches!(e, Expr::Lit(_) | Expr::Param(_))
}

/// Finds the access term: the first conjunct, in list order, that compares
/// an indexed field with literals or parameters.
pub fn access_term(
    pred: &Expr,
    index_of: &dyn Fn(&FieldName) -> Option<IndexKind>,
) -> Option<AccessTerm> {
    let conjuncts: Vec<&Expr> = match pred {
        Expr::And(xs) => xs.iter().collect(),
        other => vec![other],
    };
    for (i, c) in conjuncts.iter().enumerate() {
        let found = match c {
            Expr::Cmp { op, l, r } => {
                let (field, other) = match (&**l, &**r) {
                    (Expr::Field(f), o) if literalish(o) => (f, o),
                    (o, Expr::Field(f)) if literalish(o) => (f, o),
                    _ => continue,
                };
                let _ = other;
                match (op, index_of(field)) {
                    (CmpOp::Eq, Some(IndexKind::Lookup | IndexKind::Unique | IndexKind::Range)) => {
                        Some((field.clone(), AccessKind::Equality))
                    }
                    (CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge, Some(IndexKind::Range)) => {
                        Some((field.clone(), AccessKind::Range))
                    }
                    _ => None,
                }
            }
            Expr::In { expr, list } => match &**expr {
                Expr::Field(f) if list.iter().all(literalish) => match index_of(f) {
                    Some(IndexKind::Lookup | IndexKind::Unique | IndexKind::Range) => {
                        Some((f.clone(), AccessKind::Equality))
                    }
                    _ => None,
                },
                _ => None,
            },
            Expr::Contains { set, value } => match (&**set, &**value) {
                (Expr::Field(f), v)
                    if literalish(v) && index_of(f) == Some(IndexKind::SetMember) =>
                {
                    Some((f.clone(), AccessKind::Member))
                }
                _ => None,
            },
            _ => None,
        };
        if let Some((field, kind)) = found {
            return Some(AccessTerm {
                field,
                conjunct: i,
                kind,
            });
        }
    }
    None
}

/// Checks every value in a request for its domain, recursively through
/// expressions (U+0000, Float and Timestamp ranges).
pub fn check_expr_domain(e: &Expr) -> Result<(), crate::value::DomainError> {
    match e {
        Expr::Lit(v) => check_domain(v),
        Expr::Field(_) | Expr::Param(_) | Expr::Now | Expr::Json { .. } => Ok(()),
        Expr::Cast { expr, .. }
        | Expr::Not(expr)
        | Expr::IsNull(expr)
        | Expr::Bucket { expr, .. } => check_expr_domain(expr),
        Expr::Cmp { l, r, .. } | Expr::Arith { l, r, .. } => {
            check_expr_domain(l)?;
            check_expr_domain(r)
        }
        Expr::And(xs) | Expr::Or(xs) | Expr::Coalesce(xs) => {
            xs.iter().try_for_each(check_expr_domain)
        }
        Expr::In { expr, list } => {
            check_expr_domain(expr)?;
            list.iter().try_for_each(check_expr_domain)
        }
        Expr::Contains { set, value } => {
            check_expr_domain(set)?;
            check_expr_domain(value)
        }
        Expr::SetAdd { set, values } | Expr::SetRemove { set, values } => {
            check_expr_domain(set)?;
            values.iter().try_for_each(check_expr_domain)
        }
    }
}
