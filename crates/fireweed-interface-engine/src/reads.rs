//! Claim selection and reads.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use fireweed_interface::*;

use crate::exec::row_key;
use crate::state::*;

/// Claim candidates in claim order, with the writer scan budget applied
/// (API-006 §Work limits). Returns the chosen ids and whether the scan
/// was cut short.
pub fn claim_candidates(
    q: &Queue,
    r: &ClaimRequest,
    t: i64,
    elig_t: i64,
    limits: &ImplementationLimits,
) -> (Vec<u64>, bool) {
    let now = UtcMillis(t);
    let msv = limits.max_set_values;
    let access = r
        .filter
        .as_ref()
        .and_then(|f| access_term(f, &|x| queue_index(&q.decl, x)));
    let access_conj = |f: &Expr, at: &AccessTerm| match f {
        Expr::And(xs) => xs[at.conjunct].clone(),
        other => other.clone(),
    };
    let mut pending: Vec<&Item> = q
        .items
        .values()
        .filter(|it| it.state == St::Pending)
        .filter(|it| match (&r.filter, &access) {
            (Some(f), Some(at)) => {
                let c = access_conj(f, at);
                eval_predicate(
                    &c,
                    &ItemRow {
                        item: it,
                        decl: &q.decl,
                    },
                    &r.params,
                    now,
                    msv,
                )
            }
            _ => true,
        })
        .collect();
    pending.sort_by(|a, b| claim_order(q, a, b));
    let budget = limits.max_writer_scan_rows as usize;
    let mut out = vec![];
    let mut group: Option<Option<GroupKey>> = None;
    let mut limited = false;
    for (n, it) in pending.iter().enumerate() {
        if n >= budget {
            limited = true;
            break;
        }
        if out.len() >= r.max_items as usize {
            break;
        }
        if !eligible(q, it, elig_t) {
            continue;
        }
        if let Some(g) = &r.group_key
            && it.group_key.as_ref() != Some(g)
        {
            continue;
        }
        if r.same_group_key {
            match &group {
                Some(g) if *g != it.group_key => continue,
                None => group = Some(it.group_key.clone()),
                _ => {}
            }
        }
        if let Some(f) = &r.filter
            && !eval_predicate(
                f,
                &ItemRow {
                    item: it,
                    decl: &q.decl,
                },
                &r.params,
                now,
                msv,
            )
        {
            continue;
        }
        out.push(it.id);
    }
    (out, limited)
}

fn item_view(q: &Queue, it: &Item) -> ItemView {
    ItemView {
        item_id: ItemId(it.id),
        client_item_key: ClientItemKey::new(it.key.clone()),
        state: it.item_state(),
        item_version: it.version,
        attempt_count: it.attempts,
        priority: it.priority,
        not_before: it.not_before.map(UtcMillis),
        group_key: it.group_key.clone(),
        cohort_id: None,
        gate_keys: it.gate_keys.clone(),
        lease: it.lease.as_ref().map(|l| LeaseSummary {
            worker_id: l.worker.clone(),
            lease_expires_at: UtcMillis(l.expires_at),
        }),
        terminal: it
            .terminal_at
            .filter(|_| it.is_terminal())
            .map(|ta| TerminalSummary {
                outcome: it.item_state(),
                code: it.code.clone(),
                terminal_at: UtcMillis(ta),
            }),
        metadata: it.metadata.clone(),
        body: it.body.clone(),
        fields: q
            .decl
            .fields
            .iter()
            .map(|d| {
                (
                    d.name.clone(),
                    ItemRow {
                        item: it,
                        decl: &q.decl,
                    }
                    .field(&d.name)
                    .unwrap(),
                )
            })
            .collect(),
    }
}

pub fn get(space: &Space, r: &GetRequest, t: i64) -> DpResult<GetResponse> {
    let mut results = vec![];
    for target in &r.targets {
        results.push(match target {
            GetTarget::Item {
                queue,
                item,
                include_terminal,
            } => {
                let q = space
                    .queues
                    .get(queue)
                    .ok_or_else(|| DataPlaneError::ObjectNotFound(queue.clone()))?;
                let id = match item {
                    ItemRef::Id(id) => Some(id.0),
                    ItemRef::Key(k) => q.keys.get(k.as_str()).copied(),
                    ItemRef::IdAndKey(id, k) => {
                        q.keys.get(k.as_str()).copied().filter(|x| *x == id.0)
                    }
                    _ => None,
                };
                id.and_then(|id| q.items.get(&id))
                    .filter(|it| it.is_live() || (*include_terminal && it.is_terminal()))
                    .map(|it| GetResult::Item(item_view(q, it)))
            }
            GetTarget::Row { table, key } => {
                let tb = space
                    .tables
                    .get(table)
                    .ok_or_else(|| DataPlaneError::ObjectNotFound(table.clone()))?;
                let k: Option<Vec<KeyVal>> = key.iter().map(KeyVal::from_value).collect();
                k.and_then(|k| tb.rows.get(&k)).map(|row| {
                    let mut fields = row.fields.clone();
                    for d in &tb.decl.fields {
                        fields.entry(d.name.clone()).or_insert(Value::Null(d.ty));
                    }
                    GetResult::Row(RowView {
                        fields,
                        row_version: row.version,
                    })
                })
            }
            GetTarget::QueueState { queue } => {
                let q = space
                    .queues
                    .get(queue)
                    .ok_or_else(|| DataPlaneError::ObjectNotFound(queue.clone()))?;
                Some(GetResult::QueueState(QueueStateView {
                    paused: q.paused,
                    drain_intake: q.drain_intake,
                    blocked_gates: q.blocked.iter().map(|(k, e)| (k.clone(), *e)).collect(),
                }))
            }
            _ => {
                return Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
                    ShapeKind::Blob,
                )));
            }
        });
    }
    Ok(GetResponse {
        as_of: UtcMillis(t),
        results,
    })
}

pub fn metrics(q: &Queue, t: i64) -> QueueMetrics {
    let mut m = QueueMetrics {
        as_of: UtcMillis(t),
        pending: 0,
        leased: 0,
        complete: 0,
        failed: 0,
        retry_backlog: 0,
        oldest_eligible_age_ms: None,
        progress_bound_risk_count: 0,
        active_leases: 0,
        recurring_pending: q.decl.spec.recurrence.then_some(0),
        recurring_leased: q.decl.spec.recurrence.then_some(0),
    };
    let mut oldest: Option<i64> = None;
    for it in q.items.values() {
        match it.state {
            St::Pending => {
                m.pending += 1;
                if it.attempts > 0 {
                    m.retry_backlog += 1;
                }
                if let Some(r) = &mut m.recurring_pending {
                    *r += 1;
                }
                if !q.paused && eligible(q, it, t) {
                    let since = it.not_before.unwrap_or(it.created_at).max(it.created_at);
                    oldest = Some(oldest.map_or(since, |o| o.min(since)));
                    if let Some(bound) = q.decl.spec.progress_bound_ms
                        && t - since >= bound as i64
                    {
                        m.progress_bound_risk_count += 1;
                    }
                }
            }
            St::Leased => {
                m.leased += 1;
                m.active_leases += 1;
                if let Some(r) = &mut m.recurring_leased {
                    *r += 1;
                }
            }
            St::Complete => m.complete += 1,
            St::Failed => m.failed += 1,
            _ => {}
        }
    }
    m.oldest_eligible_age_ms = oldest.map(|o| (t - o).max(0) as u64);
    m
}

pub fn discover(space: &Space, r: &DiscoverRequest, t: i64) -> DiscoverResponse {
    let mut scopes = vec![];
    for (name, q) in &space.queues {
        if r.queues.as_ref().is_some_and(|allow| !allow.contains(name)) || q.paused {
            continue;
        }
        let mut by_group: BTreeMap<Option<GroupKey>, (i64, u64)> = BTreeMap::new();
        for it in q.items.values().filter(|it| eligible(q, it, t)) {
            if r.group_key
                .as_ref()
                .is_some_and(|g| it.group_key.as_ref() != Some(g))
            {
                continue;
            }
            let since = it.not_before.unwrap_or(it.created_at).max(it.created_at);
            let key = if r.granularity == Granularity::Group {
                it.group_key.clone()
            } else {
                None
            };
            let e = by_group.entry(key).or_insert((since, 0));
            e.0 = e.0.min(since);
            e.1 += 1;
        }
        for (g, (since, n)) in by_group {
            scopes.push(ActiveScope {
                queue: name.clone(),
                group_key: g,
                oldest_eligible_age_ms: (t - since).max(0) as u64,
                eligible_count: Some(n),
                observed_at: UtcMillis(t),
            });
        }
    }
    scopes.sort_by(|a, b| {
        b.oldest_eligible_age_ms
            .cmp(&a.oldest_eligible_age_ms)
            .then_with(|| a.queue.cmp(&b.queue))
            .then_with(|| a.group_key.cmp(&b.group_key))
    });
    let offset: usize = r
        .page_token
        .as_ref()
        .and_then(|p| p.as_str().parse().ok())
        .unwrap_or(0);
    let total = scopes.len();
    let page: Vec<ActiveScope> = scopes
        .into_iter()
        .skip(offset)
        .take(r.max_results as usize)
        .collect();
    let next = offset + page.len();
    DiscoverResponse {
        scopes: page,
        next_page_token: (next < total).then(|| PageToken::new(next.to_string())),
        partial: false,
    }
}

/// A candidate row: its key, its fields, and whether the filter selected it.
type RowEntry = (Vec<Value>, BTreeMap<FieldName, Value>, Value);

/// Group-by values and the rows in the group.
type Group<'a> = (Vec<Value>, Vec<&'a BTreeMap<FieldName, Value>>);

// ------------------------------------------------------------ declared reads

fn text_param(r: &ReadRequest, name: &str) -> Option<String> {
    match r.params.get(&ParamName::new(name)) {
        Some(Value::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

fn builtin(
    space: &Space,
    r: &ReadRequest,
    t: i64,
    limits: &ImplementationLimits,
) -> DpResult<ReadResponse> {
    let qn = ObjectName::new(
        text_param(r, "queue").ok_or_else(|| DataPlaneError::InvalidRequest("queue".into()))?,
    );
    let q = space
        .queues
        .get(&qn)
        .ok_or_else(|| DataPlaneError::ObjectNotFound(qn.clone()))?;
    let row = |pairs: Vec<(&str, Value)>| {
        pairs
            .into_iter()
            .map(|(k, v)| (FieldName::new(k), v))
            .collect()
    };
    match (r.name.as_str(), r.version) {
        ("fw_queue_counts", 1) => {
            let m = metrics(q, t);
            let rows = [
                ("pending", m.pending),
                ("leased", m.leased),
                ("complete", m.complete),
                ("failed", m.failed),
            ]
            .into_iter()
            .map(|(s, n)| {
                row(vec![
                    ("fw_state", Value::text(s)),
                    ("count", Value::Int(n as i64)),
                ])
            })
            .collect();
            Ok(ReadResponse {
                as_of: UtcMillis(t),
                rows,
                next_page_token: None,
                scan_limited: false,
            })
        }
        ("fw_gates", 1) => {
            let rows = q
                .blocked
                .iter()
                .map(|(k, e)| {
                    row(vec![
                        ("gate_key", Value::text(k)),
                        ("gate_epoch", Value::Int(*e as i64)),
                    ])
                })
                .collect();
            Ok(ReadResponse {
                as_of: UtcMillis(t),
                rows,
                next_page_token: None,
                scan_limited: false,
            })
        }
        ("fw_items", 1) => {
            let state = text_param(r, "state");
            let group = text_param(r, "group_key");
            let worker = text_param(r, "worker_id");
            let prefix = text_param(r, "key_prefix");
            let after: Option<u64> = match &r.page_token {
                None => None,
                Some(p) => Some(
                    p.as_str()
                        .parse()
                        .map_err(|_| DataPlaneError::InvalidRequest("page token".into()))?,
                ),
            };
            let all: Vec<&Item> = q
                .items
                .values()
                .filter(|it| after.is_none_or(|a| it.id > a))
                .filter(|it| it.is_live() || it.is_terminal())
                .filter(|it| {
                    state
                        .as_deref()
                        .is_none_or(|s| it.item_state().as_str() == s)
                })
                .filter(|it| {
                    group
                        .as_deref()
                        .is_none_or(|g| it.group_key.as_ref().map(|x| x.as_str()) == Some(g))
                })
                .filter(|it| {
                    worker
                        .as_deref()
                        .is_none_or(|w| it.lease.as_ref().map(|l| l.worker.as_str()) == Some(w))
                })
                .filter(|it| {
                    prefix
                        .as_deref()
                        .is_none_or(|p| it.keyed && it.key.starts_with(p))
                })
                .collect();
            let page = limits.max_read_rows as usize;
            let rows: Vec<_> = all
                .iter()
                .take(page)
                .map(|it| {
                    let view = ItemRow {
                        item: it,
                        decl: &q.decl,
                    };
                    let mut m: BTreeMap<FieldName, Value> = QUEUE_SYSTEM_FIELDS
                        .iter()
                        .map(|(n, _)| {
                            (FieldName::new(*n), view.field(&FieldName::new(*n)).unwrap())
                        })
                        .collect();
                    m.insert(
                        FieldName::new("fw_priority"),
                        view.field(&FieldName::new("fw_priority")).unwrap(),
                    );
                    for d in &q.decl.fields {
                        m.insert(d.name.clone(), view.field(&d.name).unwrap());
                    }
                    m
                })
                .collect();
            let next = (all.len() > page).then(|| PageToken::new(all[page - 1].id.to_string()));
            Ok(ReadResponse {
                as_of: UtcMillis(t),
                rows,
                next_page_token: next,
                scan_limited: false,
            })
        }
        _ => Err(DataPlaneError::ReadNotDeclared {
            name: r.name.clone(),
            version: r.version,
        }),
    }
}

pub fn read(
    space: &Space,
    r: &ReadRequest,
    t: i64,
    limits: &ImplementationLimits,
) -> DpResult<ReadResponse> {
    if r.name.as_str().starts_with("fw_") {
        return builtin(space, r, t, limits);
    }
    let decl = space
        .schema
        .reads
        .iter()
        .find(|d| d.name == r.name && d.version == r.version)
        .ok_or_else(|| DataPlaneError::ReadNotDeclared {
            name: r.name.clone(),
            version: r.version,
        })?;
    for (p, ty) in &decl.params {
        match r.params.get(p) {
            Some(v) if v.ty() == *ty => {
                check_domain(v).map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?
            }
            _ => {
                return Err(DataPlaneError::InvalidRequest(format!(
                    "parameter {p} missing or mistyped"
                )));
            }
        }
    }
    let now = UtcMillis(t);
    let msv = limits.max_set_values;
    // Rows as field maps, each with its key for ordering.
    let mut rows: Vec<RowEntry> = vec![];
    let collect = |row: &dyn RowAccess, fields: &[FieldName]| -> BTreeMap<FieldName, Value> {
        fields
            .iter()
            .map(|f| {
                (
                    f.clone(),
                    row.field(f).unwrap_or(Value::Null(ScalarType::Json)),
                )
            })
            .collect()
    };
    let all_fields: Vec<FieldName>;
    if let Some(q) = space.queues.get(&decl.source) {
        let at = access_term(&decl.filter, &|f| queue_index(&q.decl, f))
            .ok_or(DataPlaneError::SelectorNotIndexed)?;
        all_fields = QUEUE_SYSTEM_FIELDS
            .iter()
            .map(|(n, _)| FieldName::new(*n))
            .chain(std::iter::once(FieldName::new("fw_priority")))
            .chain(q.decl.fields.iter().map(|d| d.name.clone()))
            .collect();
        for it in q
            .items
            .values()
            .filter(|it| it.is_live() || it.is_terminal())
        {
            let view = ItemRow {
                item: it,
                decl: &q.decl,
            };
            let acc = match &decl.filter {
                Expr::And(xs) => xs[at.conjunct].clone(),
                other => other.clone(),
            };
            if !eval_predicate(&acc, &view, &r.params, now, msv) {
                continue;
            }
            let selected = eval_predicate(&decl.filter, &view, &r.params, now, msv);
            rows.push((
                vec![Value::Int(it.id as i64)],
                collect(&view, &all_fields),
                Value::Bool(selected),
            ));
        }
    } else if let Some(tb) = space.tables.get(&decl.source) {
        let at = access_term(&decl.filter, &|f| table_index(&tb.decl, f))
            .ok_or(DataPlaneError::SelectorNotIndexed)?;
        all_fields = ["fw_row_version", "fw_created_at", "fw_updated_at"]
            .iter()
            .map(|n| FieldName::new(*n))
            .chain(tb.decl.fields.iter().map(|d| d.name.clone()))
            .collect();
        for r0 in tb.rows.values() {
            let view = TableRow {
                row: r0,
                decl: &tb.decl,
            };
            let acc = match &decl.filter {
                Expr::And(xs) => xs[at.conjunct].clone(),
                other => other.clone(),
            };
            if !eval_predicate(&acc, &view, &r.params, now, msv) {
                continue;
            }
            let selected = eval_predicate(&decl.filter, &view, &r.params, now, msv);
            let key = row_key(&tb.decl, &r0.fields)
                .map(|k| {
                    k.into_iter()
                        .map(|kv| match kv {
                            KeyVal::Int(i) => Value::Int(i),
                            KeyVal::Text(s) => Value::Text(s),
                            KeyVal::Bytes(b) => Value::Bytes(b.into()),
                            KeyVal::Ts(t) => Value::Timestamp(UtcMillis(t)),
                        })
                        .collect()
                })
                .unwrap_or_default();
            rows.push((key, collect(&view, &all_fields), Value::Bool(selected)));
        }
    } else {
        return Err(DataPlaneError::ObjectNotFound(decl.source.clone()));
    }
    let key_cmp = |a: &[Value], b: &[Value]| {
        a.iter()
            .zip(b)
            .map(|(x, y)| sort_compare(x, y))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    };
    match &decl.output {
        ReadOutput::Rows { fields } => {
            rows.sort_by(|a, b| {
                for (f, dir) in &decl.order {
                    let (x, y) = (&a.1[f], &b.1[f]);
                    let o = sort_compare(x, y);
                    let o = if *dir == Direction::Desc {
                        o.reverse()
                    } else {
                        o
                    };
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                key_cmp(&a.0, &b.0)
            });
            // The page token is the position of the last record examined,
            // in result order (order fields, then key).
            let position = |row: &RowEntry| -> (Vec<Value>, Vec<Value>) {
                (
                    decl.order.iter().map(|(f, _)| row.1[f].clone()).collect(),
                    row.0.clone(),
                )
            };
            let cmp_pos = |a: &(Vec<Value>, Vec<Value>), b: &(Vec<Value>, Vec<Value>)| {
                for (i, (_, dir)) in decl.order.iter().enumerate() {
                    let o = sort_compare(&a.0[i], &b.0[i]);
                    let o = if *dir == Direction::Desc {
                        o.reverse()
                    } else {
                        o
                    };
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                key_cmp(&a.1, &b.1)
            };
            let start = match &r.page_token {
                None => 0,
                Some(p) => {
                    use base64::Engine as _;
                    let after: (Vec<Value>, Vec<Value>) = base64::engine::general_purpose::STANDARD
                        .decode(p.as_str())
                        .ok()
                        .and_then(|b| serde_json::from_slice(&b).ok())
                        .ok_or_else(|| DataPlaneError::InvalidRequest("page token".into()))?;
                    rows.iter()
                        .position(|row| cmp_pos(&position(row), &after) == Ordering::Greater)
                        .unwrap_or(rows.len())
                }
            };
            let token = |row: &RowEntry| {
                use base64::Engine as _;
                PageToken::new(
                    base64::engine::general_purpose::STANDARD
                        .encode(serde_json::to_vec(&position(row)).expect("serialize")),
                )
            };
            let budget = limits.max_read_scan_rows as usize;
            let mut out = vec![];
            let mut next = None;
            let mut scan_limited = false;
            for (examined, (i, row)) in rows.iter().enumerate().skip(start).enumerate() {
                if out.len() >= decl.max_rows as usize || examined >= budget {
                    scan_limited = examined >= budget && out.len() < decl.max_rows as usize;
                    next = Some(token(&rows[i - 1]));
                    break;
                }
                if row.2 == Value::Bool(true) {
                    out.push(
                        fields
                            .iter()
                            .map(|f| (f.clone(), row.1[f].clone()))
                            .collect(),
                    );
                }
            }
            Ok(ReadResponse {
                as_of: now,
                rows: out,
                next_page_token: next,
                scan_limited,
            })
        }
        ReadOutput::Aggregate { group_by, values } => {
            if rows.len() as u64 > limits.max_read_scan_rows {
                return Err(DataPlaneError::ScanLimitExceeded);
            }
            let env = {
                let mut env = MapTypeEnv::default();
                if let Some(q) = space.queues.get(&decl.source) {
                    env = queue_type_env(&q.decl, &r.params);
                } else if let Some(tb) = space.tables.get(&decl.source) {
                    env = table_type_env(&tb.decl, &r.params);
                }
                env
            };
            let selected: Vec<&BTreeMap<FieldName, Value>> = rows
                .iter()
                .filter(|(_, _, s)| *s == Value::Bool(true))
                .map(|(_, m, _)| m)
                .collect();
            let ecx = |m: &BTreeMap<FieldName, Value>, e: &Expr| -> Value {
                let cx = EvalCtx {
                    row: m,
                    params: &r.params,
                    now,
                    max_set_values: msv,
                    mode: FaultMode::Null,
                };
                eval(e, &cx).unwrap_or(Value::Null(ScalarType::Json))
            };
            // Group rows by their group-by values (nulls form one group).
            let mut groups: Vec<Group<'_>> = vec![];
            for m in &selected {
                let gk: Vec<Value> = group_by.iter().map(|(_, e)| ecx(m, e)).collect();
                match groups.iter_mut().find(|(k, _)| k == &gk) {
                    Some((_, v)) => v.push(m),
                    None => groups.push((gk, vec![m])),
                }
            }
            if group_by.is_empty() && groups.is_empty() {
                groups.push((vec![], vec![]));
            }
            groups.sort_by(|a, b| key_cmp(&a.0, &b.0));
            let mut out = vec![];
            for (gk, members) in groups {
                let mut row: BTreeMap<FieldName, Value> =
                    group_by.iter().map(|(n, _)| n.clone()).zip(gk).collect();
                for (name, agg) in values {
                    let ty = aggregate_type(agg, &env)
                        .map_err(|e| DataPlaneError::InvalidRequest(e.0))?;
                    let operand = match agg {
                        Aggregate::CountOf(e)
                        | Aggregate::Sum(e)
                        | Aggregate::Min(e)
                        | Aggregate::Max(e)
                        | Aggregate::Avg(e) => Some(e),
                        Aggregate::Percentile { expr, .. } => Some(expr),
                        _ => None,
                    };
                    let vals: Vec<Value> = match operand {
                        Some(e) => members.iter().map(|m| ecx(m, e)).collect(),
                        None => vec![],
                    };
                    let v = aggregate(agg, members.len() as u64, &vals, ty)
                        .map_err(DataPlaneError::Evaluation)?;
                    row.insert(name.clone(), v);
                }
                out.push(row);
            }
            Ok(ReadResponse {
                as_of: now,
                rows: out,
                next_page_token: None,
                scan_limited: false,
            })
        }
        _ => Err(DataPlaneError::InvalidRequest("read output".into())),
    }
}
