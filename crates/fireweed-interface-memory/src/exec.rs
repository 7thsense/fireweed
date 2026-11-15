//! Execution of write operations on a working copy of a space.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use fireweed_interface::*;
use serde::{Deserialize, Serialize};

use crate::state::*;

/// Per-request execution context.
pub(crate) struct Cx<'a> {
    pub t: i64,
    pub limits: &'a ImplementationLimits,
    pub params: &'a Params,
    pub space_key: &'a SpaceKey,
    /// Items whose `item_version` this request already incremented.
    pub bumped: BTreeSet<(ObjectName, u64)>,
}

impl Cx<'_> {
    fn eval_cx<'b>(&'b self, row: &'b dyn RowAccess, mode: FaultMode) -> EvalCtx<'b> {
        EvalCtx {
            row,
            params: self.params,
            now: UtcMillis(self.t),
            max_set_values: self.limits.max_set_values,
            mode,
        }
    }

    fn bump(&mut self, q: &ObjectName, it: &mut Item) {
        if self.bumped.insert((q.clone(), it.id)) {
            it.version += 1;
        }
        it.updated_at = self.t;
    }
}

fn new_token(space_next: &mut u64) -> String {
    *space_next += 1;
    format!("lt-{}", *space_next)
}

fn body_size(b: &Option<Body>) -> u64 {
    match b {
        Some(Body::Bytes(x)) => x.len() as u64,
        Some(Body::Json(j)) => serde_json::to_vec(j)
            .map(|v| v.len() as u64)
            .unwrap_or(u64::MAX),
        _ => 0,
    }
}

/// Constraint checks on an item after a change (API-006 §Rejecting
/// conditions, last paragraph).
pub(crate) fn check_item(
    q: &Queue,
    it: &Item,
    limits: &ImplementationLimits,
) -> Result<(), EntryStatus> {
    if body_size(&it.body) > limits.max_body_bytes {
        return Err(EntryStatus::ValueTooLarge);
    }
    if let Some(g) = &q.decl.spec.gates
        && it.gate_keys.len() > g.max_gate_keys_per_item as usize
    {
        return Err(EntryStatus::Invalid {
            reason: "too many gate keys".into(),
        });
    }
    for d in &q.decl.fields {
        if d.source != FieldSource::Stored {
            continue;
        }
        let v = it.fields.get(&d.name).cloned().unwrap_or(Value::Null(d.ty));
        if v.is_null() && !d.nullable {
            return Err(EntryStatus::MissingField {
                field: d.name.clone(),
            });
        }
        if v.byte_size() > limits.max_value_bytes {
            return Err(EntryStatus::ValueTooLarge);
        }
        if d.index == IndexHint::Unique && !v.is_null() {
            let dup = q.items.values().any(|o| {
                o.id != it.id
                    && !matches!(o.state, St::Superseded | St::Tombstone)
                    && o.fields
                        .get(&d.name)
                        .is_some_and(|ov| compare(ov, &v) == Some(Ordering::Equal))
            });
            if dup {
                return Err(EntryStatus::UniqueViolation {
                    field: d.name.clone(),
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn check_row(
    tb: &Table,
    key: &[KeyVal],
    row: &Row,
    limits: &ImplementationLimits,
) -> Result<(), EntryStatus> {
    for d in &tb.decl.fields {
        let v = row
            .fields
            .get(&d.name)
            .cloned()
            .unwrap_or(Value::Null(d.ty));
        if v.is_null() && !d.nullable {
            return Err(EntryStatus::MissingField {
                field: d.name.clone(),
            });
        }
        if v.byte_size() > limits.max_value_bytes {
            return Err(EntryStatus::ValueTooLarge);
        }
        if d.index == IndexHint::Unique && !v.is_null() {
            let dup = tb.rows.iter().any(|(k, o)| {
                k.as_slice() != key
                    && o.fields
                        .get(&d.name)
                        .is_some_and(|ov| compare(ov, &v) == Some(Ordering::Equal))
            });
            if dup {
                return Err(EntryStatus::UniqueViolation {
                    field: d.name.clone(),
                });
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------- patch

fn pointer_tokens(p: &str) -> Vec<String> {
    if p.is_empty() {
        return vec![];
    }
    p.trim_start_matches('/')
        .split('/')
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// Navigates to the parent of the last token, creating missing objects.
fn parent_mut<'a>(
    root: &'a mut serde_json::Value,
    tokens: &[String],
) -> Result<&'a mut serde_json::Value, EvalError> {
    let mut cur = root;
    for tok in tokens {
        cur = match cur {
            serde_json::Value::Object(map) => map
                .entry(tok.clone())
                .or_insert_with(|| serde_json::Value::Object(Default::default())),
            serde_json::Value::Array(a) => {
                let i: usize = tok.parse().map_err(|_| EvalError::PointerConflict)?;
                a.get_mut(i).ok_or(EvalError::PointerConflict)?
            }
            _ => return Err(EvalError::PointerConflict),
        };
    }
    Ok(cur)
}

/// Applies one patch (API-006 §Patch). Returns `true` if the body is now
/// removed (root `Remove`).
pub(crate) fn apply_patch(
    body: &mut serde_json::Value,
    patch: &BodyPatch,
    value: Option<serde_json::Value>,
) -> Result<bool, EvalError> {
    match patch {
        BodyPatch::Set { pointer, .. } => {
            let toks = pointer_tokens(pointer.as_str());
            let v = value.unwrap_or(serde_json::Value::Null);
            let Some((last, parents)) = toks.split_last() else {
                *body = v;
                return Ok(false);
            };
            match parent_mut(body, parents)? {
                serde_json::Value::Object(map) => {
                    map.insert(last.clone(), v);
                }
                serde_json::Value::Array(a) => {
                    if last == "-" {
                        a.push(v);
                    } else {
                        let i: usize = last.parse().map_err(|_| EvalError::PointerConflict)?;
                        match i.cmp(&a.len()) {
                            Ordering::Less => a[i] = v,
                            Ordering::Equal => a.push(v),
                            Ordering::Greater => return Err(EvalError::PointerConflict),
                        }
                    }
                }
                _ => return Err(EvalError::PointerConflict),
            }
            Ok(false)
        }
        BodyPatch::Remove { pointer } => {
            let toks = pointer_tokens(pointer.as_str());
            let Some((last, parents)) = toks.split_last() else {
                return Ok(true);
            };
            if let Some(parent) = body.pointer_mut(&parents_pointer(parents)) {
                match parent {
                    serde_json::Value::Object(map) => {
                        map.remove(last);
                    }
                    serde_json::Value::Array(a) => {
                        if let Ok(i) = last.parse::<usize>()
                            && i < a.len()
                        {
                            a.remove(i);
                        }
                    }
                    _ => {}
                }
            }
            Ok(false)
        }
        BodyPatch::Append {
            pointer, keep_last, ..
        } => {
            let toks = pointer_tokens(pointer.as_str());
            let v = value.unwrap_or(serde_json::Value::Null);
            let target = match toks.split_last() {
                None => body,
                Some((last, parents)) => match parent_mut(body, parents)? {
                    serde_json::Value::Object(map) => map
                        .entry(last.clone())
                        .or_insert_with(|| serde_json::Value::Array(vec![])),
                    serde_json::Value::Array(a) => {
                        let i: usize = last.parse().map_err(|_| EvalError::PointerConflict)?;
                        a.get_mut(i).ok_or(EvalError::PointerConflict)?
                    }
                    _ => return Err(EvalError::PointerConflict),
                },
            };
            match target {
                serde_json::Value::Array(a) => {
                    a.push(v);
                    if let Some(k) = keep_last {
                        let k = *k as usize;
                        if a.len() > k {
                            a.drain(..a.len() - k);
                        }
                    }
                    Ok(false)
                }
                _ => Err(EvalError::PointerConflict),
            }
        }
        _ => Ok(false),
    }
}

fn parents_pointer(parents: &[String]) -> String {
    parents
        .iter()
        .map(|t| format!("/{}", t.replace('~', "~0").replace('/', "~1")))
        .collect()
}

fn patch_value(p: &BodyPatch) -> Option<&Expr> {
    match p {
        BodyPatch::Set { value, .. } | BodyPatch::Append { value, .. } => Some(value),
        _ => None,
    }
}

/// Applies patches in order; each value sees the item as changed so far.
pub(crate) fn apply_patches(
    it: &mut Item,
    decl: &QueueDecl,
    patches: &[BodyPatch],
    cx: &Cx<'_>,
) -> Result<(), EntryStatus> {
    for p in patches {
        let value = match patch_value(p) {
            Some(e) => {
                let row = ItemRow { item: it, decl };
                let v = eval(e, &cx.eval_cx(&row, FaultMode::Error))
                    .map_err(EntryStatus::EvaluationFault)?;
                Some(value_to_json(&v))
            }
            None => None,
        };
        let mut body = match it.body.take() {
            Some(Body::Json(j)) => j,
            _ => serde_json::Value::Object(Default::default()),
        };
        let removed = apply_patch(&mut body, p, value).map_err(EntryStatus::EvaluationFault)?;
        it.body = if removed {
            None
        } else {
            Some(Body::Json(body))
        };
    }
    Ok(())
}

/// `Set`: every right-hand side sees the record as it was before.
pub(crate) fn apply_item_set(
    it: &mut Item,
    decl: &QueueDecl,
    sets: &[(FieldName, Expr)],
    cx: &Cx<'_>,
) -> Result<(), EntryStatus> {
    let mut out = Vec::with_capacity(sets.len());
    {
        let row = ItemRow { item: it, decl };
        for (f, e) in sets {
            let v = eval(e, &cx.eval_cx(&row, FaultMode::Error))
                .map_err(EntryStatus::EvaluationFault)?;
            out.push((f.clone(), v));
        }
    }
    for (f, v) in out {
        it.fields.insert(f, normalize_value(v)?);
    }
    Ok(())
}

/// Normalizes a value before it is stored (API-006 §Canonical JSON).
pub(crate) fn normalize_value(v: Value) -> Result<Value, EntryStatus> {
    match v {
        Value::Json(j) => normalize_json(&j)
            .map(Value::Json)
            .map_err(|e| EntryStatus::Invalid {
                reason: e.to_string(),
            }),
        other => Ok(other),
    }
}

fn normalize_fields(
    fs: &BTreeMap<FieldName, Value>,
) -> Result<BTreeMap<FieldName, Value>, EntryStatus> {
    fs.iter()
        .map(|(k, v)| normalize_value(v.clone()).map(|v| (k.clone(), v)))
        .collect()
}

fn normalize_metadata(m: &Metadata) -> Result<Metadata, EntryStatus> {
    m.iter()
        .map(|(k, v)| {
            normalize_json(v)
                .map(|n| (k.clone(), n))
                .map_err(|e| EntryStatus::Invalid {
                    reason: e.to_string(),
                })
        })
        .collect()
}

fn normalize_body(b: &Option<Body>) -> Result<Option<Body>, EntryStatus> {
    Ok(match b {
        Some(Body::Json(j)) => Some(Body::Json(normalize_json(j).map_err(|e| {
            EntryStatus::Invalid {
                reason: e.to_string(),
            }
        })?)),
        other => other.clone(),
    })
}

// ----------------------------------------------------------------- enqueue

pub(crate) struct EnqueueOutcome {
    pub result: EntryResult,
}

pub(crate) fn enqueue_item(
    space_next_id: &mut u64,
    q: &mut Queue,
    item: &NewItem,
    policy: KeyConflict,
    cx: &mut Cx<'_>,
) -> Result<EnqueueOutcome, EntryStatus> {
    if q.drain_intake {
        return Err(EntryStatus::Invalid {
            reason: "queue intake is drained".into(),
        });
    }
    let mut superseded = None;
    if let Some(key) = &item.client_item_key
        && let Some(&id) = q.keys.get(key.as_str())
    {
        let existing = &q.items[&id];
        match policy {
            KeyConflict::Duplicate => {
                return Ok(EnqueueOutcome {
                    result: EntryResult {
                        status: EntryStatus::Duplicate,
                        item_id: Some(ItemId(id)),
                        superseded_item_id: None,
                        version: Some(existing.version),
                        lease: None,
                    },
                });
            }
            KeyConflict::ReplacePending => match existing.state {
                St::Pending => superseded = Some(id),
                St::Leased => return Err(EntryStatus::Conflict),
                _ => {}
            },
            _ => return Err(EntryStatus::Conflict),
        }
    }
    *space_next_id += 1;
    let id = *space_next_id;
    let key = match &item.client_item_key {
        Some(k) => k.as_str().to_owned(),
        None => format!("fw#{id}"),
    };
    let new = Item {
        id,
        key: key.clone(),
        keyed: item.client_item_key.is_some(),
        state: St::Pending,
        priority: item.priority,
        not_before: item.not_before.as_ref().map(|s| resolve_schedule(s, cx.t)),
        group_key: item.group_key.clone(),
        gate_keys: item.gate_keys.clone(),
        metadata: normalize_metadata(&item.metadata)?,
        body: normalize_body(&item.body)?,
        fields: normalize_fields(&item.fields)?,
        attempts: 0,
        version: 1,
        lease: None,
        created_at: cx.t,
        updated_at: cx.t,
        terminal_at: None,
        code: None,
    };
    if let Some(old) = superseded
        && let Some(o) = q.items.get_mut(&old)
    {
        o.state = St::Superseded;
        o.updated_at = cx.t;
    }
    check_item(q, &new, cx.limits)?;
    q.items.insert(id, new);
    cx.bumped.insert((q.decl.name.clone(), id));
    if item.client_item_key.is_some() {
        q.keys.insert(key, id);
    }
    Ok(EnqueueOutcome {
        result: EntryResult {
            status: if superseded.is_some() {
                EntryStatus::Replaced
            } else {
                EntryStatus::Accepted
            },
            item_id: Some(ItemId(id)),
            superseded_item_id: superseded.map(ItemId),
            version: Some(1),
            lease: None,
        },
    })
}

// --------------------------------------------------------------- leasing

/// Resolves a lease target to an item id with a valid lease, or the
/// status explaining why not.
pub(crate) fn valid_lease(q: &Queue, target: &LeaseTarget) -> Result<u64, EntryStatus> {
    match target {
        LeaseTarget::Item(l) => match q.items.get(&l.item_id.0) {
            None => Err(EntryStatus::StaleLease),
            Some(it) => match it.state {
                St::Leased
                    if it
                        .lease
                        .as_ref()
                        .is_some_and(|x| x.token == l.lease_token.as_str()) =>
                {
                    Ok(it.id)
                }
                St::Complete | St::Failed | St::Tombstone => Err(EntryStatus::Terminal),
                St::Superseded => Err(EntryStatus::Superseded),
                _ => Err(EntryStatus::StaleLease),
            },
        },
        _ => Err(EntryStatus::StaleLease),
    }
}

pub(crate) fn lease_item(
    next_token: &mut u64,
    q: &mut Queue,
    id: u64,
    worker: &WorkerId,
    duration_ms: u64,
    cx: &mut Cx<'_>,
) -> ClaimedItem {
    let qname = q.decl.name.clone();
    let it = q.items.get_mut(&id).expect("item exists");
    let token = new_token(next_token);
    it.state = St::Leased;
    it.lease = Some(Lease {
        token: token.clone(),
        worker: worker.clone(),
        expires_at: cx.t + duration_ms as i64,
    });
    it.attempts += 1;
    cx.bump(&qname, it);
    claimed_view(it, token)
}

pub(crate) fn claimed_view(it: &Item, token: String) -> ClaimedItem {
    ClaimedItem {
        item_id: ItemId(it.id),
        client_item_key: ClientItemKey::new(it.key.clone()),
        lease_token: Some(LeaseToken::new(token)),
        lease_expires_at: UtcMillis(it.lease.as_ref().map(|l| l.expires_at).unwrap_or(0)),
        item_version: it.version,
        attempt_count: it.attempts,
        priority: it.priority,
        not_before: it.not_before.map(UtcMillis),
        group_key: it.group_key.clone(),
        cohort_id: None,
        gate_keys: it.gate_keys.clone(),
        metadata: it.metadata.clone(),
        body: it.body.clone(),
        fields: it.fields.clone(),
    }
}

pub(crate) fn renew_one(
    q: &mut Queue,
    target: &LeaseTarget,
    duration_ms: u64,
    cx: &mut Cx<'_>,
) -> EntryResult {
    match valid_lease(q, target) {
        Err(s) => EntryResult::status(s),
        Ok(id) => {
            let qname = q.decl.name.clone();
            let it = q.items.get_mut(&id).expect("item");
            let lease = it.lease.as_mut().expect("leased");
            lease.expires_at = cx.t + duration_ms as i64;
            let token = lease.token.clone();
            cx.bump(&qname, it);
            EntryResult {
                status: EntryStatus::Renewed,
                item_id: Some(ItemId(id)),
                superseded_item_id: None,
                version: Some(it.version),
                lease: Some(NewLease {
                    lease_token: LeaseToken::new(token),
                    lease_expires_at: UtcMillis(cx.t + duration_ms as i64),
                    item_version: it.version,
                }),
            }
        }
    }
}

pub(crate) fn reassign_one(
    next_token: &mut u64,
    q: &mut Queue,
    target: &LeaseRef,
    to: &WorkerId,
    duration_ms: u64,
    cx: &mut Cx<'_>,
) -> EntryResult {
    match valid_lease(q, &LeaseTarget::Item(target.clone())) {
        Err(s) => EntryResult::status(s),
        Ok(id) => {
            let c = lease_item(next_token, q, id, to, duration_ms, cx);
            EntryResult {
                status: EntryStatus::Reassigned,
                item_id: Some(ItemId(id)),
                superseded_item_id: None,
                version: Some(c.item_version),
                lease: Some(NewLease {
                    lease_token: c.lease_token.clone().expect("item lease"),
                    lease_expires_at: c.lease_expires_at,
                    item_version: c.item_version,
                }),
            }
        }
    }
}

// --------------------------------------------------------------- finalize

pub(crate) fn finalize_one(q: &mut Queue, e: &FinalizeEntry, cx: &mut Cx<'_>) -> EntryResult {
    let id = match valid_lease(q, &e.target) {
        Ok(id) => id,
        Err(s) => return EntryResult::status(s),
    };
    let mut it = q.items[&id].clone();
    if let Some(v) = e.expected_item_version
        && v != it.version
    {
        return EntryResult::status(EntryStatus::VersionMismatch);
    }
    let r = (|| -> Result<EntryStatus, EntryStatus> {
        if let Some(m) = &e.metadata {
            it.metadata = normalize_metadata(m)?;
        }
        if e.body.is_some() {
            it.body = normalize_body(&e.body)?;
        }
        apply_patches(&mut it, &q.decl, &e.patch, cx)?;
        apply_item_set(&mut it, &q.decl, &e.set, cx)?;
        let spec = &q.decl.spec;
        it.lease = None;
        let status = match &e.outcome {
            FinalizeOutcome::Complete => {
                it.state = St::Complete;
                it.terminal_at = Some(cx.t);
                it.code = None;
                EntryStatus::Completed
            }
            FinalizeOutcome::Fail { failure_code } => {
                it.state = St::Failed;
                it.terminal_at = Some(cx.t);
                it.code = Some(failure_code.clone());
                EntryStatus::Failed
            }
            FinalizeOutcome::Retry {
                not_before,
                priority,
            } => {
                if it.attempts >= spec.max_attempts {
                    it.state = St::Failed;
                    it.terminal_at = Some(cx.t);
                    it.code = Some("attempts-exhausted".into());
                    EntryStatus::Failed
                } else {
                    it.state = St::Pending;
                    it.not_before = Some(resolve_schedule(not_before, cx.t));
                    if let Some(p) = priority {
                        it.priority = *p;
                    }
                    EntryStatus::Retried
                }
            }
            FinalizeOutcome::Release => {
                it.state = St::Pending;
                EntryStatus::Released
            }
            FinalizeOutcome::Rearm {
                not_before,
                priority,
            } => {
                if !spec.recurrence {
                    return Err(EntryStatus::Invalid {
                        reason: "queue is not recurring".into(),
                    });
                }
                it.state = St::Pending;
                it.attempts = 0;
                it.not_before = Some(resolve_schedule(not_before, cx.t));
                if let Some(p) = priority {
                    it.priority = *p;
                }
                EntryStatus::Rearmed
            }
            _ => {
                return Err(EntryStatus::Invalid {
                    reason: "unsupported outcome".into(),
                });
            }
        };
        check_item(q, &it, cx.limits)?;
        Ok(status)
    })();
    match r {
        Err(s) => EntryResult::status(s),
        Ok(status) => {
            let qname = q.decl.name.clone();
            cx.bump(&qname, &mut it);
            let version = it.version;
            q.items.insert(id, it);
            EntryResult {
                status,
                item_id: Some(ItemId(id)),
                superseded_item_id: None,
                version: Some(version),
                lease: None,
            }
        }
    }
}

// ----------------------------------------------------------------- tables

pub(crate) fn row_key(
    decl: &TableDecl,
    fields: &BTreeMap<FieldName, Value>,
) -> Option<Vec<KeyVal>> {
    decl.key
        .iter()
        .map(|k| fields.get(k).and_then(KeyVal::from_value))
        .collect()
}

fn expiry_for(decl: &TableDecl, fields: &BTreeMap<FieldName, Value>, t: i64) -> Option<i64> {
    match &decl.expiry {
        Some(RowExpiry::AfterWrite { ttl_ms }) => Some(t + *ttl_ms as i64),
        Some(RowExpiry::AtField(f)) => match fields.get(f) {
            Some(Value::Timestamp(ts)) => Some(ts.0),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn put_row(
    tb: &mut Table,
    fields: &BTreeMap<FieldName, Value>,
    policy: RowConflict,
    cx: &Cx<'_>,
) -> Result<EntryResult, EntryStatus> {
    let fields = &normalize_fields(fields)?;
    let key = row_key(&tb.decl, fields).ok_or(EntryStatus::MissingField {
        field: tb.decl.key[0].clone(),
    })?;
    let (status, row) = match tb.rows.get(&key) {
        Some(existing) => match policy {
            RowConflict::Reject => return Err(EntryStatus::Conflict),
            RowConflict::Skip => {
                return Ok(EntryResult {
                    status: EntryStatus::Skipped,
                    item_id: None,
                    superseded_item_id: None,
                    version: Some(existing.version),
                    lease: None,
                });
            }
            RowConflict::Replace => {
                let mut r = existing.clone();
                r.fields = fields.clone();
                r.version += 1;
                r.updated_at = cx.t;
                r.expires_at = expiry_for(&tb.decl, &r.fields, cx.t);
                (EntryStatus::Updated, r)
            }
            _ => {
                let mut r = existing.clone();
                let mut changed = false;
                for (k, v) in fields {
                    if r.fields.get(k) != Some(v) {
                        r.fields.insert(k.clone(), v.clone());
                        changed = true;
                    }
                }
                if !changed {
                    return Ok(EntryResult {
                        status: EntryStatus::Skipped,
                        item_id: None,
                        superseded_item_id: None,
                        version: Some(existing.version),
                        lease: None,
                    });
                }
                r.version += 1;
                r.updated_at = cx.t;
                r.expires_at = expiry_for(&tb.decl, &r.fields, cx.t);
                (EntryStatus::Updated, r)
            }
        },
        None => {
            let base = tb.hwm.get(&key).map(|(v, _)| *v).unwrap_or(0);
            let r = Row {
                fields: fields.clone(),
                version: base + 1,
                created_at: cx.t,
                updated_at: cx.t,
                expires_at: expiry_for(&tb.decl, fields, cx.t),
            };
            (EntryStatus::Inserted, r)
        }
    };
    check_row(tb, &key, &row, cx.limits)?;
    let version = row.version;
    tb.rows.insert(key.clone(), row);
    tb.hwm.remove(&key);
    Ok(EntryResult {
        status,
        item_id: None,
        superseded_item_id: None,
        version: Some(version),
        lease: None,
    })
}

// ----------------------------------------------------------------- mutate

/// Record key for ordering and continuations.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum RecKey {
    Item(u64),
    Row(Vec<KeyVal>),
}

#[derive(Serialize, Deserialize)]
struct ContinuationBody {
    fp: String,
    after_value: Value,
    after_key: RecKey,
}

fn continuation_fp(cx: &Cx<'_>, m: &Mutate, schema_version: u64) -> String {
    continuation_fp_for(cx.space_key, m, schema_version, cx.params)
}

pub(crate) fn continuation_fp_for(
    space: &SpaceKey,
    m: &Mutate,
    schema_version: u64,
    params: &Params,
) -> String {
    fingerprint(&(
        space,
        &m.target,
        schema_version,
        &m.selection,
        &m.action,
        &m.lease_policy,
        params,
    ))
}

/// True if a continuation was issued for this request's selection.
pub(crate) fn continuation_matches(
    space: &SpaceKey,
    m: &Mutate,
    schema_version: u64,
    params: &Params,
    c: &Continuation,
) -> bool {
    let fp = continuation_fp_for(space, m, schema_version, params);
    base64::engine::general_purpose::STANDARD
        .decode(c.as_str())
        .ok()
        .and_then(|b| serde_json::from_slice::<ContinuationBody>(&b).ok())
        .is_some_and(|b| b.fp == fp)
}

pub(crate) enum MutateFail {
    Reason(FailureReason, Option<u32>),
}

fn fail(reason: FailureReason) -> MutateFail {
    MutateFail::Reason(reason, None)
}

fn fail_at(reason: FailureReason, i: usize) -> MutateFail {
    MutateFail::Reason(reason, Some(i as u32))
}

/// Range conjuncts on the access field, or the access conjunct alone.
fn access_conjuncts(pred: &Expr, at: &AccessTerm) -> Vec<Expr> {
    let conjuncts: Vec<Expr> = match pred {
        Expr::And(xs) => xs.clone(),
        other => vec![other.clone()],
    };
    if at.kind == AccessKind::Range {
        conjuncts
            .into_iter()
            .filter(|c| match c {
                Expr::Cmp {
                    op: CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge,
                    l,
                    r,
                } => {
                    matches!(&**l, Expr::Field(f) if *f == at.field)
                        || matches!(&**r, Expr::Field(f) if *f == at.field)
                }
                _ => false,
            })
            .collect()
    } else {
        vec![conjuncts[at.conjunct].clone()]
    }
}

pub(crate) struct Candidate {
    pub key: RecKey,
    pub access: Value,
}

/// Candidates of a `Where` predicate in access-term order.
pub(crate) fn where_candidates(
    space: &Space,
    target: &ObjectName,
    pred: &Expr,
    cx: &Cx<'_>,
) -> Result<Vec<Candidate>, DataPlaneError> {
    let params = cx.params;
    let now = UtcMillis(cx.t);
    let msv = cx.limits.max_set_values;
    let mut out = Vec::new();
    if let Some(q) = space.queues.get(target) {
        let at = access_term(pred, &|f| queue_index(&q.decl, f))
            .ok_or(DataPlaneError::SelectorNotIndexed)?;
        let acc = access_conjuncts(pred, &at);
        for it in q.items.values() {
            if matches!(it.state, St::Superseded | St::Tombstone) {
                continue;
            }
            let row = ItemRow {
                item: it,
                decl: &q.decl,
            };
            if acc
                .iter()
                .all(|c| eval_predicate(c, &row, params, now, msv))
            {
                let access = if at.kind == AccessKind::Member {
                    Value::Null(ScalarType::Text)
                } else {
                    row.field(&at.field).unwrap()
                };
                out.push(Candidate {
                    key: RecKey::Item(it.id),
                    access,
                });
            }
        }
    } else if let Some(tb) = space.tables.get(target) {
        let at = access_term(pred, &|f| table_index(&tb.decl, f))
            .ok_or(DataPlaneError::SelectorNotIndexed)?;
        let acc = access_conjuncts(pred, &at);
        for (k, r) in &tb.rows {
            let row = TableRow {
                row: r,
                decl: &tb.decl,
            };
            if acc
                .iter()
                .all(|c| eval_predicate(c, &row, params, now, msv))
            {
                let access = if at.kind == AccessKind::Member {
                    Value::Null(ScalarType::Text)
                } else {
                    row.field(&at.field).unwrap()
                };
                out.push(Candidate {
                    key: RecKey::Row(k.clone()),
                    access,
                });
            }
        }
    } else {
        return Err(DataPlaneError::ObjectNotFound(target.clone()));
    }
    out.sort_by(|a, b| sort_compare(&a.access, &b.access).then_with(|| a.key.cmp(&b.key)));
    Ok(out)
}

fn record_matches(
    space: &Space,
    target: &ObjectName,
    key: &RecKey,
    pred: Option<&Expr>,
    cx: &Cx<'_>,
) -> bool {
    let Some(pred) = pred else { return true };
    let (params, now, msv) = (cx.params, UtcMillis(cx.t), cx.limits.max_set_values);
    match key {
        RecKey::Item(id) => {
            let q = &space.queues[target];
            eval_predicate(
                pred,
                &ItemRow {
                    item: &q.items[id],
                    decl: &q.decl,
                },
                params,
                now,
                msv,
            )
        }
        RecKey::Row(k) => {
            let tb = &space.tables[target];
            eval_predicate(
                pred,
                &TableRow {
                    row: &tb.rows[k],
                    decl: &tb.decl,
                },
                params,
                now,
                msv,
            )
        }
    }
}

fn record_version(space: &Space, target: &ObjectName, key: &RecKey) -> Option<u64> {
    match key {
        RecKey::Item(id) => space.queues.get(target)?.items.get(id).map(|i| i.version),
        RecKey::Row(k) => space.tables.get(target)?.rows.get(k).map(|r| r.version),
    }
}

fn resolve_item_ref(q: &Queue, r: &ItemRef) -> Result<Option<u64>, ()> {
    let live = |id: u64| {
        q.items
            .get(&id)
            .filter(|i| !matches!(i.state, St::Superseded | St::Tombstone))
            .map(|i| i.id)
    };
    match r {
        ItemRef::Id(id) => Ok(live(id.0)),
        ItemRef::Key(k) => Ok(q.keys.get(k.as_str()).and_then(|id| live(*id))),
        ItemRef::IdAndKey(id, k) => {
            let by_key = q.keys.get(k.as_str()).copied();
            if by_key.is_some() && by_key != Some(id.0) {
                return Err(());
            }
            Ok(live(id.0))
        }
        _ => Ok(None),
    }
}

/// Executes a `Mutate` on the working space.
pub(crate) fn exec_mutate(
    space: &mut Space,
    m: &Mutate,
    cx: &mut Cx<'_>,
) -> Result<(OpResult, Option<Continuation>), MutateFail> {
    let mut res = OpResult {
        evaluated: true,
        ..Default::default()
    };
    let is_queue = space.queues.contains_key(&m.target);
    // Selection.
    let mut selected: Vec<RecKey> = Vec::new();
    let mut continuation = None;
    let chunked = m.chunk.is_some();
    match &m.selection {
        Selection::Keys { keys, filter } => {
            let mut seen = BTreeSet::new();
            for (i, kd) in keys.iter().enumerate() {
                let rk = match (&kd.key, is_queue) {
                    (KeyRef::Item(r), true) => {
                        match resolve_item_ref(&space.queues[&m.target], r) {
                            Err(()) => {
                                return Err(fail_at(
                                    FailureReason::Entry(EntryStatus::Conflict),
                                    i,
                                ));
                            }
                            Ok(id) => id.map(RecKey::Item),
                        }
                    }
                    (KeyRef::Row(vals), false) => {
                        let k: Option<Vec<KeyVal>> = vals.iter().map(KeyVal::from_value).collect();
                        k.filter(|k| space.tables[&m.target].rows.contains_key(k))
                            .map(RecKey::Row)
                    }
                    _ => {
                        return Err(fail_at(
                            FailureReason::Entry(EntryStatus::Invalid {
                                reason: "key kind".into(),
                            }),
                            i,
                        ));
                    }
                };
                match (&rk, kd.expected_version) {
                    (Some(_), Some(0)) | (None, Some(1..)) => {
                        return Err(fail_at(
                            FailureReason::Entry(EntryStatus::VersionMismatch),
                            i,
                        ));
                    }
                    (Some(k), Some(v)) if record_version(space, &m.target, k) != Some(v) => {
                        return Err(fail_at(
                            FailureReason::Entry(EntryStatus::VersionMismatch),
                            i,
                        ));
                    }
                    _ => {}
                }
                match rk {
                    None => res.entries.push(EntryResult::status(EntryStatus::NotFound)),
                    Some(k) => {
                        if seen.insert(k.clone())
                            && record_matches(space, &m.target, &k, filter.as_ref(), cx)
                        {
                            selected.push(k);
                        }
                    }
                }
            }
        }
        Selection::Where { predicate } => {
            let cands = where_candidates(space, &m.target, predicate, cx)
                .map_err(|_| fail(FailureReason::ScanLimitExceeded))?;
            let schema_version = space.schema.version;
            let fp = continuation_fp(cx, m, schema_version);
            let start = match &m.chunk {
                Some(Chunk::Continue(c)) => {
                    let body: ContinuationBody = base64::engine::general_purpose::STANDARD
                        .decode(c.as_str())
                        .ok()
                        .and_then(|b| serde_json::from_slice(&b).ok())
                        .ok_or(fail(FailureReason::Entry(EntryStatus::Invalid {
                            reason: "continuation".into(),
                        })))?;
                    if body.fp != fp {
                        return Err(fail(FailureReason::Entry(EntryStatus::Invalid {
                            reason: "InvalidContinuation".into(),
                        })));
                    }
                    cands
                        .iter()
                        .position(|c| {
                            sort_compare(&c.access, &body.after_value)
                                .then_with(|| c.key.cmp(&body.after_key))
                                == Ordering::Greater
                        })
                        .unwrap_or(cands.len())
                }
                _ => 0,
            };
            let budget = cx.limits.max_writer_scan_rows as usize;
            let remaining = &cands[start..];
            if !chunked && remaining.len() > budget {
                return Err(fail(FailureReason::ScanLimitExceeded));
            }
            let mut last = None;
            for c in remaining.iter().take(budget) {
                last = Some(c);
                if record_matches(space, &m.target, &c.key, Some(predicate), cx) {
                    selected.push(c.key.clone());
                    if chunked && selected.len() >= m.limit as usize {
                        break;
                    }
                }
            }
            if chunked && let Some(l) = last {
                let idx = remaining.iter().position(|c| c.key == l.key).unwrap_or(0);
                if idx + 1 < remaining.len() {
                    let body = ContinuationBody {
                        fp,
                        after_value: l.access.clone(),
                        after_key: l.key.clone(),
                    };
                    let enc = base64::engine::general_purpose::STANDARD
                        .encode(serde_json::to_vec(&body).unwrap());
                    continuation = Some(Continuation::new(enc));
                }
            }
        }
        _ => {
            return Err(fail(FailureReason::Entry(EntryStatus::Invalid {
                reason: "selection".into(),
            })));
        }
    }
    // Lease policy, counting.
    res.matched = selected.len() as u64;
    if !chunked && res.matched > m.limit as u64 {
        return Err(fail(FailureReason::LimitExceeded {
            matched_at_least: res.matched,
        }));
    }
    if let Some(e) = m.expect
        && (res.matched < e.min || res.matched > e.max)
    {
        return Err(fail(FailureReason::MatchExpectUnmet {
            matched: res.matched,
        }));
    }
    for (i, k) in selected.iter().enumerate() {
        let entry =
            match k {
                RecKey::Item(id) => apply_item_action(space, &m.target, *id, m, cx, &mut res)?,
                RecKey::Row(key) => apply_row_action(space, &m.target, key, m, cx, &mut res)
                    .map_err(|s| match entry_fail(s) {
                        MutateFail::Reason(r, _) => MutateFail::Reason(r, Some(i as u32)),
                    })?,
            };
        if let Some(e) = entry {
            res.entries.push(e);
        }
    }
    Ok((res, continuation))
}

fn transition_status(t: &Transition) -> EntryStatus {
    match t {
        Transition::Complete { .. } => EntryStatus::Completed,
        Transition::Fail { .. } => EntryStatus::Failed,
        Transition::Retry { .. } => EntryStatus::Retried,
        Transition::Release => EntryStatus::Released,
        _ => EntryStatus::Redriven,
    }
}

fn apply_item_action(
    space: &mut Space,
    target: &ObjectName,
    id: u64,
    m: &Mutate,
    cx: &mut Cx<'_>,
    res: &mut OpResult,
) -> Result<Option<EntryResult>, MutateFail> {
    let q = space.queues.get(target).expect("queue");
    let mut it = q.items[&id].clone();
    let leased = it.state == St::Leased;
    if leased {
        match &m.lease_policy {
            LeasePolicy::Skip => {
                res.skipped_leased += 1;
                return Ok(Some(EntryResult {
                    item_id: Some(ItemId(id)),
                    ..EntryResult::status(EntryStatus::Skipped)
                }));
            }
            LeasePolicy::Reject => return Err(fail(FailureReason::LeasedMatch)),
            LeasePolicy::MatchToken(tok) => {
                if it.lease.as_ref().map(|l| l.token.as_str()) != Some(tok.as_str()) {
                    return Err(fail(FailureReason::Entry(EntryStatus::StaleLease)));
                }
            }
            LeasePolicy::Break => {
                it.lease = None;
                it.state = St::Pending;
            }
            _ => {}
        }
    } else if let LeasePolicy::MatchToken(_) = &m.lease_policy {
        return Err(fail(FailureReason::Entry(EntryStatus::StaleLease)));
    }
    let terminal = it.is_terminal();
    let decl = q.decl.clone();
    let inapplicable = |res: &mut OpResult| {
        res.inapplicable += 1;
        Ok(Some(EntryResult {
            item_id: Some(ItemId(id)),
            ..EntryResult::status(EntryStatus::Inapplicable)
        }))
    };
    let ev = |e: &Expr, it: &Item, cx: &Cx<'_>| -> Result<Value, MutateFail> {
        let row = ItemRow {
            item: it,
            decl: &decl,
        };
        eval(e, &cx.eval_cx(&row, FaultMode::Error)).map_err(|x| fail(FailureReason::Evaluation(x)))
    };
    let status = match &m.action {
        Action::Set(sets) => {
            apply_item_set(&mut it, &decl, sets, cx).map_err(entry_fail)?;
            EntryStatus::Updated
        }
        Action::Patch(ps) => {
            apply_patches(&mut it, &decl, ps, cx).map_err(entry_fail)?;
            EntryStatus::Updated
        }
        Action::ReplaceBody(b) => {
            it.body = normalize_body(b).map_err(entry_fail)?;
            EntryStatus::Updated
        }
        Action::SetMetadata(changes) => {
            for (k, v) in changes {
                match v {
                    Some(v) => {
                        let n = normalize_json(v).map_err(|e| {
                            entry_fail(EntryStatus::Invalid {
                                reason: e.to_string(),
                            })
                        })?;
                        it.metadata.insert(k.clone(), n);
                    }
                    None => {
                        it.metadata.remove(k);
                    }
                }
            }
            EntryStatus::Updated
        }
        Action::Reschedule {
            not_before,
            priority,
        } => {
            if terminal {
                return inapplicable(res);
            }
            match not_before {
                ScheduleChange::Clear => it.not_before = None,
                ScheduleChange::Set(e) => match ev(e, &it, cx)? {
                    Value::Timestamp(ts) => it.not_before = Some(ts.0),
                    _ => it.not_before = None,
                },
                _ => {}
            }
            if let Some(p) = priority {
                match ev(p, &it, cx)? {
                    Value::Int(i) => it.priority = PriorityValue::Int(i),
                    Value::Timestamp(ts) => it.priority = PriorityValue::Timestamp(ts),
                    _ => {
                        return Err(entry_fail(EntryStatus::MissingField {
                            field: FieldName::new("fw_priority"),
                        }));
                    }
                }
            }
            EntryStatus::Updated
        }
        Action::Gates { add, remove } => {
            if terminal {
                return inapplicable(res);
            }
            it.gate_keys.extend(add.iter().cloned());
            for g in remove {
                it.gate_keys.remove(g);
            }
            EntryStatus::Updated
        }
        Action::Transition(tr) => {
            let ok = match tr {
                Transition::Redrive => terminal,
                Transition::Release => it.state == St::Leased,
                _ => !terminal,
            };
            if !ok {
                return inapplicable(res);
            }
            match tr {
                Transition::Complete { code } => {
                    it.state = St::Complete;
                    it.terminal_at = Some(cx.t);
                    it.code = code.clone();
                }
                Transition::Fail { failure_code } => {
                    it.state = St::Failed;
                    it.terminal_at = Some(cx.t);
                    it.code = Some(failure_code.clone());
                }
                Transition::Retry { not_before } => {
                    let nb = match ev(not_before, &it, cx)? {
                        Value::Timestamp(ts) => Some(ts.0),
                        _ => None,
                    };
                    it.state = St::Pending;
                    it.not_before = nb;
                }
                Transition::Release => it.state = St::Pending,
                _ => {
                    it.state = St::Pending;
                    it.attempts = 0;
                    it.terminal_at = None;
                    it.code = None;
                }
            }
            it.lease = if it.state == St::Leased {
                it.lease.take()
            } else {
                None
            };
            transition_status(tr)
        }
        Action::Retire => {
            it.state = St::Tombstone;
            it.lease = None;
            it.terminal_at = Some(cx.t);
            EntryStatus::Purged
        }
        _ => {
            return Err(entry_fail(EntryStatus::Invalid {
                reason: "action".into(),
            }));
        }
    };
    let q = space.queues.get_mut(target).expect("queue");
    check_item(q, &it, cx.limits).map_err(entry_fail)?;
    cx.bump(target, &mut it);
    let version = it.version;
    q.items.insert(id, it);
    res.affected += 1;
    Ok(Some(EntryResult {
        status,
        item_id: Some(ItemId(id)),
        superseded_item_id: None,
        version: Some(version),
        lease: None,
    }))
}

fn entry_fail(s: EntryStatus) -> MutateFail {
    match s {
        EntryStatus::EvaluationFault(e) => fail(FailureReason::Evaluation(e)),
        other => fail(FailureReason::Entry(other)),
    }
}

fn apply_row_action(
    space: &mut Space,
    target: &ObjectName,
    key: &[KeyVal],
    m: &Mutate,
    cx: &mut Cx<'_>,
    res: &mut OpResult,
) -> Result<Option<EntryResult>, EntryStatus> {
    let tb = space.tables.get_mut(target).expect("table");
    let mut row = tb.rows[key].clone();
    let status = match &m.action {
        Action::Set(sets) => {
            let mut out = Vec::new();
            {
                let view = TableRow {
                    row: &row,
                    decl: &tb.decl,
                };
                for (f, e) in sets {
                    let v = eval(e, &cx.eval_cx(&view, FaultMode::Error))
                        .map_err(EntryStatus::EvaluationFault)?;
                    out.push((f.clone(), v));
                }
            }
            for (f, v) in out {
                row.fields.insert(f, normalize_value(v)?);
            }
            row.version += 1;
            row.updated_at = cx.t;
            if let Some(RowExpiry::AfterWrite { ttl_ms }) = &tb.decl.expiry {
                row.expires_at = Some(cx.t + *ttl_ms as i64);
            }
            EntryStatus::Updated
        }
        Action::Retire => {
            let r = tb.rows.remove(key).expect("row");
            tb.hwm.insert(key.to_vec(), (r.version, cx.t));
            res.affected += 1;
            return Ok(Some(EntryResult {
                version: Some(r.version),
                ..EntryResult::status(EntryStatus::Deleted)
            }));
        }
        _ => {
            return Err(EntryStatus::Invalid {
                reason: "action not defined on tables".into(),
            });
        }
    };
    check_row(tb, key, &row, cx.limits)?;
    let version = row.version;
    tb.rows.insert(key.to_vec(), row);
    res.affected += 1;
    Ok(Some(EntryResult {
        status,
        item_id: None,
        superseded_item_id: None,
        version: Some(version),
        lease: None,
    }))
}
