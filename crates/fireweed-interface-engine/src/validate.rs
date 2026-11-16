//! Validation phase (API-006 §Phases): everything checked before state is
//! read. Failures are definitive errors and leave no request record.

use std::collections::{BTreeMap, BTreeSet};

use fireweed_interface::*;

use crate::exec::continuation_matches;
use crate::state::*;

type R = DpResult<()>;
type IndexOf = Box<dyn Fn(&FieldName) -> Option<IndexKind>>;

fn invalid<T>(msg: impl Into<String>) -> DpResult<T> {
    Err(DataPlaneError::InvalidRequest(msg.into()))
}

fn type_err(e: TypeError) -> DataPlaneError {
    DataPlaneError::InvalidRequest(e.0)
}

fn walk<'a>(e: &'a Expr, f: &mut dyn FnMut(&'a Expr)) {
    f(e);
    match e {
        Expr::Cast { expr, .. }
        | Expr::Not(expr)
        | Expr::IsNull(expr)
        | Expr::Bucket { expr, .. } => walk(expr, f),
        Expr::Cmp { l, r, .. } | Expr::Arith { l, r, .. } => {
            walk(l, f);
            walk(r, f);
        }
        Expr::And(xs) | Expr::Or(xs) | Expr::Coalesce(xs) => xs.iter().for_each(|x| walk(x, f)),
        Expr::In { expr, list } => {
            walk(expr, f);
            list.iter().for_each(|x| walk(x, f));
        }
        Expr::Contains { set, value } => {
            walk(set, f);
            walk(value, f);
        }
        Expr::SetAdd { set, values } | Expr::SetRemove { set, values } => {
            walk(set, f);
            values.iter().for_each(|x| walk(x, f));
        }
        _ => {}
    }
}

/// Typing, domain and `In`-size checks of one expression.
fn expr(
    e: &Expr,
    env: &dyn TypeEnv,
    want: Option<ScalarType>,
    limits: &ImplementationLimits,
) -> DpResult<ScalarType> {
    let t = type_of(e, env).map_err(type_err)?;
    if let Some(w) = want
        && t != w
    {
        return invalid(format!("expression has type {t:?}, expected {w:?}"));
    }
    check_expr_domain(e).map_err(|d| DataPlaneError::InvalidRequest(d.to_string()))?;
    let mut too_big = false;
    walk(e, &mut |x| {
        if let Expr::In { list, .. } = x
            && list.len() > limits.max_in_list as usize
        {
            too_big = true;
        }
    });
    if too_big {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::InList));
    }
    Ok(t)
}

fn params(p: &Params) -> R {
    for v in p.values() {
        check_domain(v).map_err(|d| DataPlaneError::InvalidRequest(d.to_string()))?;
    }
    Ok(())
}

fn value_matches(v: &Value, ty: ScalarType) -> bool {
    v.ty() == ty
}

/// No U+0000 (API-006 §Values).
pub fn text_ok(s: &str, what: &str) -> R {
    if s.contains('\0') {
        return invalid(format!("{what} contains U+0000"));
    }
    Ok(())
}

/// `Schedule::At` in the Timestamp domain; `After` within its span.
pub fn schedule_ok(s: &Schedule) -> R {
    match s {
        Schedule::At(t) if !timestamp_in_domain(t.0) => {
            invalid("schedule outside the Timestamp domain")
        }
        Schedule::After { delay_ms } if *delay_ms > (TIMESTAMP_MAX - TIMESTAMP_MIN) as u64 => {
            invalid("schedule delay too large")
        }
        _ => Ok(()),
    }
}

pub fn priority_kind_ok(q: &Queue, p: &PriorityValue) -> R {
    let pk = match p {
        PriorityValue::Int(_) => PriorityKind::Int64,
        _ => PriorityKind::Timestamp,
    };
    if pk != q.decl.spec.priority_kind {
        return invalid("priority kind does not match the queue");
    }
    Ok(())
}

pub fn priority_ok(p: &PriorityValue) -> R {
    match p {
        PriorityValue::Timestamp(t) if !timestamp_in_domain(t.0) => {
            invalid("priority outside the Timestamp domain")
        }
        _ => Ok(()),
    }
}

fn metadata_ok(m: &Metadata) -> R {
    for (k, v) in m {
        text_ok(k, "metadata key")?;
        normalize_json(v).map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?;
    }
    Ok(())
}

// ------------------------------------------------------------------ schema

fn name(s: &str, what: &str) -> R {
    if !is_valid_name(s) || is_reserved(s) {
        return invalid(format!("invalid {what} name {s:?}"));
    }
    Ok(())
}

fn fields(fs: &[FieldDecl], body: Option<BodyKind>, table_key: Option<&[FieldName]>) -> R {
    let mut seen = BTreeSet::new();
    for f in fs {
        name(f.name.as_str(), "field")?;
        if !seen.insert(&f.name) {
            return invalid(format!("duplicate field {}", f.name));
        }
        if matches!(f.source, FieldSource::BodyPointer(_)) && body != Some(BodyKind::Json) {
            return invalid("BodyPointer fields need a Json body");
        }
        if f.index == IndexHint::Unique
            && matches!(
                f.ty,
                ScalarType::TextSet | ScalarType::Json | ScalarType::Float
            )
        {
            return invalid(format!("{:?} cannot be Unique", f.ty));
        }
        match f.merge {
            MergeRule::Any => {}
            rule => {
                let Some(key) = table_key else {
                    return invalid("merge rules apply to table fields only");
                };
                if key.contains(&f.name) || f.source != FieldSource::Stored {
                    return invalid(format!(
                        "merge rule on {}: only stored, non-key fields",
                        f.name
                    ));
                }
                let ok = match rule {
                    MergeRule::AddOnly => f.ty == ScalarType::TextSet,
                    MergeRule::NonDecreasing => matches!(
                        f.ty,
                        ScalarType::Int | ScalarType::Float | ScalarType::Timestamp
                    ),
                    _ => false,
                };
                if !ok {
                    return invalid(format!("merge rule {rule:?} does not apply to {:?}", f.ty));
                }
            }
        }
    }
    Ok(())
}

/// Longest representable duration: the span of the Timestamp domain, so a
/// timestamp plus a duration cannot overflow.
const MAX_SPAN_MS: u64 = (TIMESTAMP_MAX - TIMESTAMP_MIN) as u64;

fn span_ok(ms: u64, what: &str) -> R {
    if ms > MAX_SPAN_MS {
        return invalid(format!("{what} longer than the Timestamp domain"));
    }
    Ok(())
}

pub fn schema(s: &SpaceSchema, limits: &ImplementationLimits, caps: &Capabilities) -> R {
    let unsupported = |u| Err(DataPlaneError::FeatureUnsupported(u));
    span_ok(s.request_id_retention_ms, "request_id_retention_ms")?;
    if s.request_id_retention_ms > limits.max_request_id_retention_ms {
        return Err(DataPlaneError::FeatureUnsupported(Unsupported::Limit(
            LimitKind::RequestIdRetention,
        )));
    }
    let count = s.queues.len() + s.tables.len();
    if count > limits.max_objects_per_space as usize {
        return Err(DataPlaneError::FeatureUnsupported(Unsupported::Limit(
            LimitKind::Objects,
        )));
    }
    let mut names = BTreeSet::new();
    for q in &s.queues {
        name(q.name.as_str(), "queue")?;
        if !names.insert(&q.name) {
            return invalid(format!("duplicate object {}", q.name));
        }
        if !caps.priority_kinds.contains(&q.spec.priority_kind) {
            return invalid("priority kind not supported");
        }
        if !caps.ordering_modes.contains(&q.spec.ordering) {
            return invalid("ordering mode not supported");
        }
        if q.spec.cohorts.is_some() {
            return unsupported(Unsupported::QueueFeature(QueueFeature::Cohorts));
        }
        if q.spec.group_batching {
            return unsupported(Unsupported::QueueFeature(QueueFeature::GroupBatching));
        }
        span_ok(
            q.spec.client_item_key_retention_ms,
            "client_item_key_retention_ms",
        )?;
        span_ok(q.spec.terminal_retention_ms, "terminal_retention_ms")?;
        if let Some(b) = q.spec.progress_bound_ms {
            span_ok(b, "progress_bound_ms")?;
        }
        if let Backoff::Fixed { ms } | Backoff::Exponential { max_ms: ms, .. } =
            q.spec.retry_backoff
        {
            span_ok(ms, "retry backoff")?;
        }
        if q.spec.max_attempts == 0 {
            return invalid("max_attempts must be positive");
        }
        if q.spec.default_lease_duration_ms > limits.max_lease_duration_ms {
            return Err(DataPlaneError::FeatureUnsupported(Unsupported::Limit(
                LimitKind::LeaseDuration,
            )));
        }
        fields(&q.fields, Some(q.body), None)?;
    }
    for t in &s.tables {
        name(t.name.as_str(), "table")?;
        if !names.insert(&t.name) {
            return invalid(format!("duplicate object {}", t.name));
        }
        fields(&t.fields, None, Some(&t.key))?;
        if t.key.is_empty() || t.key.len() > 4 {
            return invalid("a table key has 1 to 4 fields");
        }
        if t.key_assignment == KeyAssignment::Sequence {
            let int_key = t.key.len() == 1
                && t.fields
                    .iter()
                    .any(|f| f.name == t.key[0] && f.ty == ScalarType::Int);
            if !int_key {
                return invalid("a Sequence table's key is one non-null Int field");
            }
        }
        for k in &t.key {
            let Some(d) = t.fields.iter().find(|f| &f.name == k) else {
                return invalid(format!("key field {k} not declared"));
            };
            if d.nullable
                || !matches!(
                    d.ty,
                    ScalarType::Int | ScalarType::Text | ScalarType::Bytes | ScalarType::Timestamp
                )
                || d.source != FieldSource::Stored
            {
                return invalid(format!(
                    "key field {k} must be a non-null Int, Text, Bytes or Timestamp"
                ));
            }
        }
        if let Some(RowExpiry::AfterWrite { ttl_ms }) = &t.expiry {
            span_ok(*ttl_ms, "ttl_ms")?;
        }
        if let Some(RowExpiry::AtField(f)) = &t.expiry
            && !t
                .fields
                .iter()
                .any(|d| &d.name == f && d.ty == ScalarType::Timestamp)
        {
            return invalid("AtField expiry needs a Timestamp field");
        }
    }
    let mut read_names = BTreeSet::new();
    for r in &s.reads {
        name(r.name.as_str(), "read")?;
        if !read_names.insert((&r.name, r.version)) {
            return invalid("duplicate read version");
        }
        if r.max_rows == 0 || r.max_rows > limits.max_read_rows {
            return invalid("read max_rows outside 1 to max_read_rows");
        }
        let ps: Params = r
            .params
            .iter()
            .map(|(p, t)| (p.clone(), Value::Null(*t)))
            .collect();
        let (env, index): (MapTypeEnv, IndexOf) =
            if let Some(q) = s.queues.iter().find(|q| q.name == r.source) {
                let q2 = q.clone();
                (
                    queue_type_env(q, &ps),
                    Box::new(move |f| queue_index(&q2, f)),
                )
            } else if let Some(t) = s.tables.iter().find(|t| t.name == r.source) {
                let t2 = t.clone();
                (
                    table_type_env(t, &ps),
                    Box::new(move |f| table_index(&t2, f)),
                )
            } else {
                return invalid(format!("read source {} not declared", r.source));
            };
        expr(&r.filter, &env, Some(ScalarType::Bool), limits)?;
        let Some(at) = access_term(&r.filter, &*index) else {
            return Err(DataPlaneError::SelectorNotIndexed);
        };
        match &r.output {
            ReadOutput::Rows { fields } => {
                for f in fields {
                    if env.field_type(f).is_none() {
                        return invalid(format!("unknown field {f}"));
                    }
                }
                for (f, _) in &r.order {
                    match env.field_type(f) {
                        Some(t) if sortable(t) => {}
                        _ => return invalid(format!("field {f} cannot be ordered")),
                    }
                }
                if let Some((first, _)) = r.order.first()
                    && !(at.kind == AccessKind::Equality
                        || (at.kind == AccessKind::Range && &at.field == first))
                {
                    return Err(DataPlaneError::SelectorNotIndexed);
                }
            }
            ReadOutput::Aggregate { group_by, values } => {
                for (_, e) in group_by {
                    let t = expr(e, &env, None, limits)?;
                    if !groupable(t) {
                        return invalid(format!("{t:?} cannot be grouped"));
                    }
                }
                for (_, a) in values {
                    aggregate_type(a, &env).map_err(type_err)?;
                    // Operands get the full expression checks (domain, `In` size).
                    match a {
                        Aggregate::CountOf(e)
                        | Aggregate::Sum(e)
                        | Aggregate::Min(e)
                        | Aggregate::Max(e)
                        | Aggregate::Avg(e)
                        | Aggregate::Percentile { expr: e, .. } => {
                            expr(e, &env, None, limits)?;
                        }
                        _ => {}
                    }
                }
            }
            _ => return invalid("read output"),
        }
    }
    Ok(())
}

pub fn additive(old: &SpaceSchema, new: &SpaceSchema) -> R {
    let conflict = || {
        Err(DataPlaneError::SchemaConflict {
            current_version: Some(old.version),
        })
    };
    if new.request_id_retention_ms < old.request_id_retention_ms {
        return conflict();
    }
    // An old field may gain an index; nothing else about it may change.
    let field_ok = |o: &FieldDecl, n: &FieldDecl| {
        o.name == n.name
            && o.ty == n.ty
            && o.nullable == n.nullable
            && o.source == n.source
            && o.merge == n.merge
            && (o.index == n.index || (o.index == IndexHint::None && n.index != IndexHint::Unique))
    };
    let fields_ok = |o: &[FieldDecl], n: &[FieldDecl]| {
        o.iter().all(|f| {
            n.iter().filter(|x| x.name == f.name).count() == 1 && n.iter().any(|x| field_ok(f, x))
        }) && n
            .iter()
            .filter(|f| !o.iter().any(|x| x.name == f.name))
            .all(|f| f.nullable)
    };
    // Retentions may only be raised; the rest of the spec is fixed.
    let spec_ok = |o: &QueueSpec, n: &QueueSpec| {
        let mut relaxed = n.clone();
        relaxed.client_item_key_retention_ms = o.client_item_key_retention_ms;
        relaxed.terminal_retention_ms = o.terminal_retention_ms;
        relaxed == *o
            && n.client_item_key_retention_ms >= o.client_item_key_retention_ms
            && n.terminal_retention_ms >= o.terminal_retention_ms
    };
    for q in &old.queues {
        match new.queues.iter().find(|n| n.name == q.name) {
            Some(n)
                if spec_ok(&q.spec, &n.spec)
                    && n.body == q.body
                    && fields_ok(&q.fields, &n.fields) => {}
            _ => return conflict(),
        }
    }
    for t in &old.tables {
        match new.tables.iter().find(|n| n.name == t.name) {
            Some(n)
                if n.key == t.key
                    && n.expiry == t.expiry
                    && n.key_assignment == t.key_assignment
                    && fields_ok(&t.fields, &n.fields) => {}
            _ => return conflict(),
        }
    }
    for r in &old.reads {
        if !new.reads.contains(r) {
            return conflict();
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ queues

fn item_fields(q: &Queue, fs: &BTreeMap<FieldName, Value>) -> R {
    for (f, v) in fs {
        let Some(d) = q.decl.fields.iter().find(|d| &d.name == f) else {
            return invalid(format!("unknown field {f}"));
        };
        if d.source != FieldSource::Stored {
            return invalid(format!("field {f} is derived"));
        }
        if !value_matches(v, d.ty) {
            return invalid(format!("field {f} expects {:?}", d.ty));
        }
        check_domain(v).map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?;
    }
    Ok(())
}

fn body_kind(q: &Queue, b: &Option<Body>) -> R {
    match (b, q.decl.body) {
        (None, _)
        | (Some(Body::Bytes(_)), BodyKind::Bytes)
        | (Some(Body::Json(_)), BodyKind::Json) => {}
        _ => return invalid("body kind does not match the queue"),
    }
    if let Some(Body::Json(j)) = b {
        normalize_json(j).map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?;
    }
    Ok(())
}

pub fn new_items(q: &Queue, items: &[NewItem]) -> R {
    for it in items {
        let pk = match it.priority {
            PriorityValue::Int(_) => PriorityKind::Int64,
            _ => PriorityKind::Timestamp,
        };
        if pk != q.decl.spec.priority_kind {
            return invalid("priority kind does not match the queue");
        }
        if it.cohort.is_some() {
            return Err(DataPlaneError::FeatureUnsupported(
                Unsupported::QueueFeature(QueueFeature::Cohorts),
            ));
        }
        if !it.gate_keys.is_empty() && q.decl.spec.gates.is_none() {
            return Err(DataPlaneError::GatesNotEnabled);
        }
        if let Some(k) = &it.client_item_key
            && (k.as_str().contains('\0') || k.as_str().starts_with("fw#"))
        {
            return invalid("invalid client item key");
        }
        metadata_ok(&it.metadata)?;
        priority_ok(&it.priority)?;
        if let Some(nb) = &it.not_before {
            schedule_ok(nb)?;
        }
        if let Some(g) = &it.group_key {
            text_ok(g.as_str(), "group key")?;
        }
        for g in &it.gate_keys {
            text_ok(g, "gate key")?;
        }
        item_fields(q, &it.fields)?;
        body_kind(q, &it.body)?;
    }
    Ok(())
}

pub fn enqueue_items(
    space: &Space,
    queue: &ObjectName,
    items: &[NewItem],
    limits: &ImplementationLimits,
) -> R {
    let q = space
        .queues
        .get(queue)
        .ok_or_else(|| DataPlaneError::ObjectNotFound(queue.clone()))?;
    if items.len() > limits.max_entries_per_commit as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
    }
    new_items(q, items)
}

/// A claim's `eligibility_time`, if given, is in the Timestamp domain.
pub fn eligibility_time(e: Option<UtcMillis>) -> R {
    match e {
        Some(e) if !timestamp_in_domain(e.0) => {
            invalid("eligibility_time outside the Timestamp domain")
        }
        _ => Ok(()),
    }
}

pub fn claim(q: &Queue, r: &ClaimRequest, limits: &ImplementationLimits) -> R {
    text_ok(r.worker_id.as_str(), "worker id")?;
    if let Some(g) = &r.group_key {
        text_ok(g.as_str(), "group key")?;
    }
    eligibility_time(r.eligibility_time)?;
    if r.max_items == 0 || r.max_items > limits.max_claim_batch_size {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::ClaimBatch));
    }
    if r.lease_duration_ms == 0 || r.lease_duration_ms > limits.max_lease_duration_ms {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::LeaseDuration));
    }
    match r.unit {
        ClaimUnit::Items => {}
        ClaimUnit::WholeCohort => {
            return Err(DataPlaneError::FeatureUnsupported(
                Unsupported::QueueFeature(QueueFeature::Cohorts),
            ));
        }
        _ => {
            return Err(DataPlaneError::FeatureUnsupported(
                Unsupported::QueueFeature(QueueFeature::GroupBatching),
            ));
        }
    }
    params(&r.params)?;
    if let Some(f) = &r.filter {
        let env = queue_type_env(&q.decl, &r.params);
        expr(f, &env, Some(ScalarType::Bool), limits)?;
        let at = access_term(f, &|x| queue_index(&q.decl, x))
            .ok_or(DataPlaneError::SelectorNotIndexed)?;
        let kind = queue_index(&q.decl, &at.field);
        if at.kind != AccessKind::Equality
            || !matches!(kind, Some(IndexKind::Lookup | IndexKind::Unique))
        {
            return Err(DataPlaneError::SelectorNotIndexed);
        }
    }
    Ok(())
}

fn set_list(
    env: &MapTypeEnv,
    decl_fields: &[FieldDecl],
    keys: &[FieldName],
    sets: &[(FieldName, Expr)],
    limits: &ImplementationLimits,
) -> R {
    let mut seen = BTreeSet::new();
    for (f, e) in sets {
        if !seen.insert(f) {
            return invalid(format!("field {f} assigned twice"));
        }
        let Some(d) = decl_fields.iter().find(|d| &d.name == f) else {
            return invalid(format!("field {f} cannot be set"));
        };
        if d.source != FieldSource::Stored || keys.contains(f) {
            return invalid(format!("field {f} cannot be set"));
        }
        expr(e, env, Some(d.ty), limits)?;
    }
    Ok(())
}

fn patches(q: &Queue, env: &MapTypeEnv, ps: &[BodyPatch], limits: &ImplementationLimits) -> R {
    if !ps.is_empty() && q.decl.body != BodyKind::Json {
        return invalid("Patch needs a Json body");
    }
    for p in ps {
        match p {
            BodyPatch::Set { value, .. } => {
                expr(value, env, None, limits)?;
            }
            BodyPatch::Append {
                value, keep_last, ..
            } => {
                if *keep_last == Some(0) {
                    return invalid("keep_last must be at least 1");
                }
                expr(value, env, None, limits)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn finalize_entries(
    q: &Queue,
    es: &[FinalizeEntry],
    p: &Params,
    limits: &ImplementationLimits,
) -> R {
    if es.len() > limits.max_entries_per_commit as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
    }
    params(p)?;
    let env = queue_type_env(&q.decl, p);
    for e in es {
        set_list(&env, &q.decl.fields, &[], &e.set, limits)?;
        patches(q, &env, &e.patch, limits)?;
        body_kind(q, &e.body)?;
        if let Some(m) = &e.metadata {
            metadata_ok(m)?;
        }
        match &e.outcome {
            FinalizeOutcome::Fail { failure_code } => text_ok(failure_code, "failure code")?,
            FinalizeOutcome::Retry {
                not_before,
                priority,
            }
            | FinalizeOutcome::Rearm {
                not_before,
                priority,
            } => {
                schedule_ok(not_before)?;
                if let Some(p) = priority {
                    priority_ok(p)?;
                    priority_kind_ok(q, p)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ commit

fn entries_of(op: &WriteOp) -> usize {
    match op {
        WriteOp::Enqueue { items, .. } => items.len(),
        WriteOp::Finalize { entries, .. } => entries.len(),
        WriteOp::Renew { leases, .. } => leases.len(),
        WriteOp::Reassign { leases, .. } => leases.len(),
        WriteOp::SetGates { gates, .. } => gates.len(),
        WriteOp::PutRows { rows, .. } => rows.len(),
        WriteOp::Mutate(Mutate { selection, .. }) | WriteOp::Assert { selection, .. } => {
            match selection {
                Selection::Keys { keys, .. } => keys.len(),
                _ => 0,
            }
        }
        _ => 1,
    }
}

fn queue<'a>(space: &'a Space, n: &ObjectName) -> DpResult<&'a Queue> {
    space
        .queues
        .get(n)
        .ok_or_else(|| DataPlaneError::ObjectNotFound(n.clone()))
}

pub fn commit(space: &Space, r: &CommitRequest, limits: &ImplementationLimits) -> R {
    if r.ops.len() > limits.max_ops_per_commit as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::Ops));
    }
    if r.ops.iter().map(entries_of).sum::<usize>() > limits.max_entries_per_commit as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
    }
    let bytes = serde_json::to_vec(r)
        .map(|v| v.len() as u64)
        .unwrap_or(u64::MAX);
    if bytes > limits.max_request_bytes {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::RequestBytes));
    }
    params(&r.params)?;
    for op in &r.ops {
        match op {
            WriteOp::Enqueue {
                queue: qn, items, ..
            } => new_items(queue(space, qn)?, items)?,
            WriteOp::Finalize { queue: qn, entries } => {
                finalize_entries(queue(space, qn)?, entries, &r.params, limits)?
            }
            WriteOp::Reassign { to_worker, .. } if to_worker.as_str().contains('\0') => {
                return invalid("worker id contains U+0000");
            }
            WriteOp::Renew {
                queue: qn,
                lease_duration_ms,
                ..
            }
            | WriteOp::Reassign {
                queue: qn,
                lease_duration_ms,
                ..
            } => {
                queue(space, qn)?;
                if *lease_duration_ms == 0 || *lease_duration_ms > limits.max_lease_duration_ms {
                    return Err(DataPlaneError::BatchTooLarge(LimitKind::LeaseDuration));
                }
            }
            WriteOp::SetGates { queue: qn, gates } => {
                let q = queue(space, qn)?;
                let Some(g) = &q.decl.spec.gates else {
                    return Err(DataPlaneError::GatesNotEnabled);
                };
                if gates.len() > g.max_gates_per_request as usize {
                    return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
                }
                if gates.iter().any(|(k, _)| k.contains('\0')) {
                    return invalid("gate key contains U+0000");
                }
            }
            WriteOp::SetQueueState { queue: qn, .. } => {
                queue(space, qn)?;
            }
            WriteOp::PutRows { table, rows, .. } => {
                let tb = space
                    .tables
                    .get(table)
                    .ok_or_else(|| DataPlaneError::ObjectNotFound(table.clone()))?;
                for row in rows {
                    for (f, v) in row {
                        let Some(d) = tb.decl.fields.iter().find(|d| &d.name == f) else {
                            return invalid(format!("unknown field {f}"));
                        };
                        if !value_matches(v, d.ty) {
                            return invalid(format!("field {f} expects {:?}", d.ty));
                        }
                        check_domain(v)
                            .map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?;
                    }
                }
            }
            WriteOp::Mutate(m) => mutate(space, r, m, limits)?,
            WriteOp::Assert {
                target,
                selection,
                expect,
            } => {
                if expect.min > expect.max {
                    return invalid("expect min above max");
                }
                // The selection rules are Mutate's; `Retire` stands in for
                // the absent action, which Assert never applies.
                let mut m = Mutate::new(
                    target.as_str(),
                    selection.clone(),
                    Action::Retire,
                    limits.max_selector_affected,
                );
                m.expect = Some(*expect);
                mutate(space, r, &m, limits)?;
            }
            _ => return invalid("unknown operation"),
        }
    }
    Ok(())
}

fn transition_result(a: &Action) -> Option<&'static str> {
    match a {
        Action::Transition(Transition::Complete { .. }) => Some("complete"),
        Action::Transition(Transition::Fail { .. }) => Some("failed"),
        Action::Transition(_) => Some("pending"),
        Action::Retire => Some("retired"),
        _ => None,
    }
}

fn literal_texts(pred: &Expr, at: &AccessTerm, params: &Params) -> Vec<String> {
    let conj = match pred {
        Expr::And(xs) => xs.get(at.conjunct).cloned(),
        other => Some(other.clone()),
    };
    let mut out = vec![];
    if let Some(c) = conj {
        walk(&c, &mut |x| match x {
            Expr::Lit(Value::Text(s)) => out.push(s.clone()),
            Expr::Param(p) => {
                if let Some(Value::Text(s)) = params.get(p) {
                    out.push(s.clone());
                }
            }
            _ => {}
        });
    }
    out
}

fn mutate(space: &Space, r: &CommitRequest, m: &Mutate, limits: &ImplementationLimits) -> R {
    if m.limit == 0 || m.limit > limits.max_selector_affected {
        return invalid("limit outside 1 to max_selector_affected");
    }
    let (env, is_queue, decl_fields, keys) = if let Some(q) = space.queues.get(&m.target) {
        (
            queue_type_env(&q.decl, &r.params),
            true,
            q.decl.fields.clone(),
            vec![],
        )
    } else if let Some(t) = space.tables.get(&m.target) {
        (
            table_type_env(&t.decl, &r.params),
            false,
            t.decl.fields.clone(),
            t.decl.key.clone(),
        )
    } else {
        return Err(DataPlaneError::ObjectNotFound(m.target.clone()));
    };
    let mut access: Option<AccessTerm> = None;
    match &m.selection {
        Selection::Keys { keys: ks, filter } => {
            for k in ks {
                match (&k.key, is_queue) {
                    (KeyRef::Item(_), true) => {}
                    (KeyRef::Row(vals), false) => {
                        if vals.len() != keys.len() {
                            return invalid("row key arity");
                        }
                        for (v, kf) in vals.iter().zip(&keys) {
                            let d = decl_fields
                                .iter()
                                .find(|d| &d.name == kf)
                                .expect("key declared");
                            if !value_matches(v, d.ty) {
                                return invalid(format!("key field {kf} expects {:?}", d.ty));
                            }
                        }
                    }
                    _ => return invalid("key kind does not match the target"),
                }
            }
            if let Some(f) = filter {
                expr(f, &env, Some(ScalarType::Bool), limits)?;
            }
        }
        Selection::Where { predicate } => {
            expr(predicate, &env, Some(ScalarType::Bool), limits)?;
            let at = if is_queue {
                let q = &space.queues[&m.target];
                access_term(predicate, &|f| queue_index(&q.decl, f))
            } else {
                let t = &space.tables[&m.target];
                access_term(predicate, &|f| table_index(&t.decl, f))
            };
            access = Some(at.ok_or(DataPlaneError::SelectorNotIndexed)?);
        }
        _ => return invalid("selection"),
    }
    if m.override_merge_rules && is_queue {
        return invalid("override_merge_rules applies to tables only");
    }
    if !is_queue
        && matches!(m.action, Action::Set(_))
        && space.tables[&m.target].decl.key_assignment == KeyAssignment::Sequence
    {
        return invalid("rows of a Sequence table are immutable");
    }
    // Action validity.
    match (&m.action, is_queue) {
        (Action::Set(sets), _) => set_list(&env, &decl_fields, &keys, sets, limits)?,
        (Action::Retire, _) => {}
        (Action::Patch(ps), true) => patches(&space.queues[&m.target], &env, ps, limits)?,
        (Action::ReplaceBody(b), true) => body_kind(&space.queues[&m.target], b)?,
        (Action::SetMetadata(changes), true) => {
            for (k, v) in changes {
                text_ok(k, "metadata key")?;
                if let Some(v) = v {
                    normalize_json(v).map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?;
                }
            }
        }
        (
            Action::Reschedule {
                not_before,
                priority,
            },
            true,
        ) => {
            if let ScheduleChange::Set(e) = not_before {
                expr(e, &env, Some(ScalarType::Timestamp), limits)?;
            }
            if let Some(p) = priority {
                let want = priority_type(space.queues[&m.target].decl.spec.priority_kind);
                expr(p, &env, Some(want), limits)?;
            }
        }
        (Action::Transition(tr), true) => match tr {
            Transition::Retry { not_before } => {
                expr(not_before, &env, Some(ScalarType::Timestamp), limits)?;
            }
            Transition::Fail { failure_code } => text_ok(failure_code, "failure code")?,
            Transition::Complete { code: Some(c) } => text_ok(c, "code")?,
            _ => {}
        },
        (Action::Gates { add, .. }, true) => {
            if space.queues[&m.target].decl.spec.gates.is_none() {
                return Err(DataPlaneError::GatesNotEnabled);
            }
            if add.iter().any(|g| g.contains('\0')) {
                return invalid("gate key contains U+0000");
            }
        }
        _ => return invalid("action not defined on this shape"),
    }
    // Lease policy.
    match &m.lease_policy {
        LeasePolicy::Preserve if matches!(m.action, Action::Transition(_) | Action::Retire) => {
            return invalid("Preserve cannot be combined with Transition or Retire");
        }
        LeasePolicy::MatchToken(_) => match &m.selection {
            Selection::Keys { keys, .. } if keys.len() == 1 => {}
            _ => return invalid("MatchToken needs a single-key selection"),
        },
        LeasePolicy::MatchCohortToken(_) => {
            return Err(DataPlaneError::FeatureUnsupported(
                Unsupported::QueueFeature(QueueFeature::Cohorts),
            ));
        }
        _ => {}
    }
    // Chunk rules.
    if let Some(chunk) = &m.chunk {
        if r.ops.len() != 1 || r.dry_run {
            return invalid("a chunked Mutate must be the only operation and cannot be a dry run");
        }
        let Some(at) = access else {
            return invalid("a chunked Mutate needs a Where selection");
        };
        let f = at.field.as_str();
        let body_derived = space.queues.get(&m.target).is_some_and(|q| {
            q.decl
                .fields
                .iter()
                .any(|d| d.name == at.field && matches!(d.source, FieldSource::BodyPointer(_)))
        });
        let changes_access = match &m.action {
            Action::Set(sets) => sets.iter().any(|(x, _)| x == &at.field),
            Action::Patch(_) | Action::ReplaceBody(_) => body_derived,
            Action::Reschedule { .. } => matches!(f, "fw_not_before" | "fw_priority"),
            Action::Gates { .. } => f == "fw_gate_keys",
            Action::Transition(_) | Action::Retire => match f {
                "fw_state" => {
                    let vals = match &m.selection {
                        Selection::Where { predicate } => literal_texts(predicate, &at, &r.params),
                        _ => vec![],
                    };
                    at.kind != AccessKind::Equality
                        || transition_result(&m.action)
                            .is_some_and(|res| vals.iter().any(|v| v == res))
                }
                "fw_terminal_at" | "fw_worker_id" | "fw_not_before" => true,
                _ => false,
            },
            _ => false,
        };
        if changes_access {
            return invalid("a chunked Mutate may not change its access field");
        }
        if let Chunk::Continue(c) = chunk
            && !continuation_matches(&r.header.space, m, space.schema.version, &r.params, c)
        {
            return Err(DataPlaneError::InvalidContinuation);
        }
    }
    Ok(())
}
