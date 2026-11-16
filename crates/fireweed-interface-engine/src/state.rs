//! In-memory state, record views and lazy time-driven transitions.

use std::collections::{BTreeMap, BTreeSet};

use fireweed_interface::*;

#[derive(Clone, Debug)]
pub struct Space {
    pub schema: SpaceSchema,
    pub queues: BTreeMap<ObjectName, Queue>,
    pub tables: BTreeMap<ObjectName, Table>,
    pub records: BTreeMap<RequestId, Record>,
    pub next_item_id: u64,
    pub next_token: u64,
    pub last_time: i64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Record {
    pub fingerprint: String,
    pub recorded_at: i64,
    pub response: RecordedResponse,
}

#[derive(Clone, Debug)]
pub struct Queue {
    pub decl: QueueDecl,
    pub items: BTreeMap<u64, Item>,
    /// Client key to item id, for live and retained items.
    pub keys: BTreeMap<String, u64>,
    pub paused: bool,
    pub drain_intake: bool,
    pub blocked: BTreeMap<String, u64>,
    pub gate_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum St {
    Pending,
    Leased,
    Complete,
    Failed,
    Tombstone,
    Superseded,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Lease {
    pub token: String,
    pub worker: WorkerId,
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Item {
    pub id: u64,
    pub key: String,
    pub keyed: bool,
    pub state: St,
    pub priority: PriorityValue,
    pub not_before: Option<i64>,
    pub group_key: Option<GroupKey>,
    pub gate_keys: BTreeSet<String>,
    pub metadata: Metadata,
    pub body: Option<Body>,
    pub fields: BTreeMap<FieldName, Value>,
    pub attempts: u32,
    pub version: u64,
    pub lease: Option<Lease>,
    pub created_at: i64,
    pub updated_at: i64,
    pub terminal_at: Option<i64>,
    pub code: Option<String>,
}

impl Item {
    pub fn is_live(&self) -> bool {
        matches!(self.state, St::Pending | St::Leased)
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self.state, St::Complete | St::Failed)
    }
    pub fn item_state(&self) -> ItemState {
        match self.state {
            St::Pending => ItemState::Pending,
            St::Leased => ItemState::Leased,
            St::Complete => ItemState::Complete,
            _ => ItemState::Failed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum KeyVal {
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
    Ts(i64),
}

impl KeyVal {
    pub fn from_value(v: &Value) -> Option<KeyVal> {
        Some(match v {
            Value::Int(i) => KeyVal::Int(*i),
            Value::Text(s) => KeyVal::Text(s.clone()),
            Value::Bytes(b) => KeyVal::Bytes(b.to_vec()),
            Value::Timestamp(t) => KeyVal::Ts(t.0),
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Table {
    pub decl: TableDecl,
    pub rows: BTreeMap<Vec<KeyVal>, Row>,
    /// Per-key version high-water mark and deletion time.
    pub hwm: BTreeMap<Vec<KeyVal>, (u64, i64)>,
    /// `KeyAssignment::Sequence`: the last key assigned (0 before the first).
    pub next_seq: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub fields: BTreeMap<FieldName, Value>,
    pub version: u64,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: Option<i64>,
}

// ----------------------------------------------------------- type envs

pub const QUEUE_SYSTEM_FIELDS: &[(&str, ScalarType)] = &[
    ("fw_item_id", ScalarType::Int),
    ("fw_key", ScalarType::Text),
    ("fw_state", ScalarType::Text),
    ("fw_not_before", ScalarType::Timestamp),
    ("fw_group_key", ScalarType::Text),
    ("fw_cohort_id", ScalarType::Text),
    ("fw_attempt_count", ScalarType::Int),
    ("fw_item_version", ScalarType::Int),
    ("fw_lease_expires_at", ScalarType::Timestamp),
    ("fw_worker_id", ScalarType::Text),
    ("fw_created_at", ScalarType::Timestamp),
    ("fw_updated_at", ScalarType::Timestamp),
    ("fw_terminal_at", ScalarType::Timestamp),
    ("fw_gate_keys", ScalarType::TextSet),
    ("fw_metadata", ScalarType::Json),
];

pub fn priority_type(kind: PriorityKind) -> ScalarType {
    match kind {
        PriorityKind::Timestamp => ScalarType::Timestamp,
        _ => ScalarType::Int,
    }
}

pub fn queue_type_env(decl: &QueueDecl, params: &Params) -> MapTypeEnv {
    let mut env = MapTypeEnv::default();
    for (n, t) in QUEUE_SYSTEM_FIELDS {
        env.fields.insert(FieldName::new(*n), *t);
    }
    env.fields.insert(
        FieldName::new("fw_priority"),
        priority_type(decl.spec.priority_kind),
    );
    for f in &decl.fields {
        env.fields.insert(f.name.clone(), f.ty);
    }
    for (p, v) in params {
        env.params.insert(p.clone(), v.ty());
    }
    env
}

pub fn table_type_env(decl: &TableDecl, params: &Params) -> MapTypeEnv {
    let mut env = MapTypeEnv::default();
    env.fields
        .insert(FieldName::new("fw_row_version"), ScalarType::Int);
    env.fields
        .insert(FieldName::new("fw_created_at"), ScalarType::Timestamp);
    env.fields
        .insert(FieldName::new("fw_updated_at"), ScalarType::Timestamp);
    for f in &decl.fields {
        env.fields.insert(f.name.clone(), f.ty);
    }
    for (p, v) in params {
        env.params.insert(p.clone(), v.ty());
    }
    env
}

fn hint_kind(f: &FieldDecl) -> Option<IndexKind> {
    match (f.index, f.ty) {
        (IndexHint::None, _) => None,
        (_, ScalarType::TextSet) => Some(IndexKind::SetMember),
        (IndexHint::Lookup, _) => Some(IndexKind::Lookup),
        (IndexHint::Range, _) => Some(IndexKind::Range),
        (IndexHint::Unique, _) => Some(IndexKind::Unique),
        _ => None,
    }
}

pub fn queue_index(decl: &QueueDecl, f: &FieldName) -> Option<IndexKind> {
    match f.as_str() {
        "fw_item_id" | "fw_key" => Some(IndexKind::Unique),
        "fw_state" | "fw_group_key" | "fw_cohort_id" | "fw_worker_id" => Some(IndexKind::Lookup),
        "fw_not_before" | "fw_terminal_at" => Some(IndexKind::Range),
        "fw_gate_keys" => Some(IndexKind::SetMember),
        _ => decl
            .fields
            .iter()
            .find(|d| &d.name == f)
            .and_then(hint_kind),
    }
}

pub fn table_index(decl: &TableDecl, f: &FieldName) -> Option<IndexKind> {
    if decl.key.first() == Some(f) {
        // A single-field key is unique; declaring it `Range` (as a
        // `Sequence` table's key usually is) also allows range access.
        let declared_range = decl
            .fields
            .iter()
            .any(|d| &d.name == f && d.index == IndexHint::Range);
        return Some(if decl.key.len() == 1 && !declared_range {
            IndexKind::Unique
        } else {
            IndexKind::Range
        });
    }
    decl.fields
        .iter()
        .find(|d| &d.name == f)
        .and_then(hint_kind)
}

// ------------------------------------------------------------- row views

pub struct ItemRow<'a> {
    pub item: &'a Item,
    pub decl: &'a QueueDecl,
}

fn opt_ts(v: Option<i64>) -> Value {
    v.map(|t| Value::Timestamp(UtcMillis(t)))
        .unwrap_or(Value::Null(ScalarType::Timestamp))
}

fn opt_text(v: Option<&str>) -> Value {
    v.map(|s| Value::Text(s.to_owned()))
        .unwrap_or(Value::Null(ScalarType::Text))
}

pub fn priority_value(p: PriorityValue) -> Value {
    match p {
        PriorityValue::Int(i) => Value::Int(i),
        PriorityValue::Timestamp(t) => Value::Timestamp(t),
        _ => Value::Null(ScalarType::Int),
    }
}

impl RowAccess for ItemRow<'_> {
    fn field(&self, f: &FieldName) -> Option<Value> {
        let it = self.item;
        Some(match f.as_str() {
            "fw_item_id" => Value::Int(it.id as i64),
            "fw_key" => Value::Text(it.key.clone()),
            "fw_state" => Value::Text(it.item_state().as_str().to_owned()),
            "fw_priority" => priority_value(it.priority),
            "fw_not_before" => opt_ts(it.not_before),
            "fw_group_key" => opt_text(it.group_key.as_ref().map(|g| g.as_str())),
            "fw_cohort_id" => Value::Null(ScalarType::Text),
            "fw_attempt_count" => Value::Int(it.attempts as i64),
            "fw_item_version" => Value::Int(it.version as i64),
            "fw_lease_expires_at" => opt_ts(it.lease.as_ref().map(|l| l.expires_at)),
            "fw_worker_id" => opt_text(it.lease.as_ref().map(|l| l.worker.as_str())),
            "fw_created_at" => Value::Timestamp(UtcMillis(it.created_at)),
            "fw_updated_at" => Value::Timestamp(UtcMillis(it.updated_at)),
            "fw_terminal_at" => opt_ts(it.terminal_at),
            "fw_gate_keys" => Value::TextSet(it.gate_keys.clone()),
            "fw_metadata" => Value::Json(serde_json::Value::Object(
                it.metadata.clone().into_iter().collect(),
            )),
            _ => {
                let d = self.decl.fields.iter().find(|d| &d.name == f)?;
                match &d.source {
                    FieldSource::BodyPointer(p) => match &it.body {
                        Some(Body::Json(j)) => match j.pointer(p.as_str()) {
                            Some(x) => cast(&Value::Json(x.clone()), d.ty),
                            None => Value::Null(d.ty),
                        },
                        _ => Value::Null(d.ty),
                    },
                    _ => it.fields.get(f).cloned().unwrap_or(Value::Null(d.ty)),
                }
            }
        })
    }
}

pub struct TableRow<'a> {
    pub row: &'a Row,
    pub decl: &'a TableDecl,
}

impl RowAccess for TableRow<'_> {
    fn field(&self, f: &FieldName) -> Option<Value> {
        Some(match f.as_str() {
            "fw_row_version" => Value::Int(self.row.version as i64),
            "fw_created_at" => Value::Timestamp(UtcMillis(self.row.created_at)),
            "fw_updated_at" => Value::Timestamp(UtcMillis(self.row.updated_at)),
            _ => {
                let d = self.decl.fields.iter().find(|d| &d.name == f)?;
                self.row.fields.get(f).cloned().unwrap_or(Value::Null(d.ty))
            }
        })
    }
}

// --------------------------------------------- lazy time-driven transitions

/// Applies every transition due at or before `t` (API-006 §Time-driven
/// transitions): lease expiry, then retention.
pub fn settle(space: &mut Space, t: i64) {
    let retention = space.schema.request_id_retention_ms as i64;
    space.records.retain(|_, r| r.recorded_at + retention > t);
    for q in space.queues.values_mut() {
        let spec = q.decl.spec.clone();
        let mut forget = Vec::new();
        for it in q.items.values_mut() {
            if it.state == St::Leased
                && let Some(l) = &it.lease
                && l.expires_at <= t
            {
                // Lease expiry: token stale, item_version unchanged. The
                // item returns to pending, unless its claims have used up
                // `max_attempts`: then it fails at the expiry instant, so a
                // worker that crashes on it every time cannot loop forever.
                let at = l.expires_at;
                it.lease = None;
                if it.attempts >= spec.max_attempts {
                    it.state = St::Failed;
                    it.terminal_at = Some(at);
                    it.code = Some("attempts-exhausted".into());
                } else {
                    it.state = St::Pending;
                }
            }
            if let Some(ta) = it.terminal_at {
                // Terminal retention ends addressability; the item becomes a
                // tombstone that still reserves its key.
                if matches!(it.state, St::Complete | St::Failed)
                    && ta + spec.terminal_retention_ms as i64 <= t
                {
                    it.state = St::Tombstone;
                }
                // Key retention ends the reservation.
                if it.state == St::Tombstone && ta + spec.client_item_key_retention_ms as i64 <= t {
                    forget.push(it.id);
                }
            }
            if it.state == St::Superseded && it.updated_at + retention <= t {
                forget.push(it.id);
            }
        }
        for id in forget {
            if let Some(it) = q.items.remove(&id)
                && q.keys.get(&it.key) == Some(&id)
            {
                q.keys.remove(&it.key);
            }
        }
    }
    for tb in space.tables.values_mut() {
        let expired: Vec<Vec<KeyVal>> = tb
            .rows
            .iter()
            .filter(|(_, r)| r.expires_at.is_some_and(|e| e <= t))
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            if let Some(r) = tb.rows.remove(&k) {
                tb.hwm.insert(k, (r.version, r.expires_at.unwrap_or(t)));
            }
        }
        tb.hwm.retain(|_, (_, at)| *at + retention > t);
    }
}

/// True if the item is eligible under API-001 Eligibility Precedence A at
/// `eligibility_time` (pause handled by the caller).
pub fn eligible(q: &Queue, it: &Item, eligibility_time: i64) -> bool {
    if it.state != St::Pending {
        return false;
    }
    if it.not_before.is_some_and(|nb| nb > eligibility_time) {
        return false;
    }
    if it.gate_keys.iter().any(|g| q.blocked.contains_key(g)) {
        return false;
    }
    for (k, blocked) in &q.decl.spec.metadata_blockers {
        if let Some(v) = it.metadata.get(k)
            && blocked.contains(v)
        {
            return false;
        }
    }
    true
}

/// Claim order: priority (by direction), then item id.
pub fn claim_order(q: &Queue, a: &Item, b: &Item) -> std::cmp::Ordering {
    let p = a.priority.cmp(&b.priority);
    let p = if q.decl.spec.priority_direction == Direction::Desc {
        p.reverse()
    } else {
        p
    };
    p.then(a.id.cmp(&b.id))
}

pub fn resolve_schedule(s: &Schedule, t: i64) -> i64 {
    match s {
        Schedule::At(at) => at.0,
        Schedule::After { delay_ms } => t.saturating_add(*delay_ms as i64),
        _ => t,
    }
}
