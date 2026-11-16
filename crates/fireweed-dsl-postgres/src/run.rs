//! Each resolved statement as one or two SQL statements (API-006
//! §Statements).

use std::collections::{BTreeMap, BTreeSet};

use fireweed_dsl::check::{
    Agg, Column, Kind, Obj as DslObj, Outcome as SettleOutcome, Output, RSelect, RStatement,
    Selection,
};
use fireweed_dsl::{
    FieldType, LeasedPolicy, MaxAttempts, MergeRule, OnConflict, OnLimit, RejectReason, Result,
};
use serde_json::{Value, json};
use tokio_postgres::Transaction;

use crate::ddl::{queue_table, table_table};
use crate::lower::{NOW, Obj, Q};
use crate::{Attempt, Space, Swept, db, refs};

pub enum Outcome {
    Done(Value),
    Rejected(RejectReason),
}

type R = std::result::Result<Outcome, Attempt>;

fn obj(sp: &Space, o: &DslObj) -> Obj {
    match o.kind {
        Kind::Queue => Obj {
            table: queue_table(&sp.schema_name, &o.name),
            kind: Kind::Queue,
            max_attempts: sp.schema.queues[&o.name].max_attempts,
            key: vec![],
        },
        Kind::Table => Obj {
            table: table_table(&sp.schema_name, &o.name),
            kind: Kind::Table,
            max_attempts: MaxAttempts::Unlimited,
            key: sp.schema.tables[&o.name].key.clone(),
        },
    }
}

async fn query(
    tx: &Transaction<'_>,
    q: &Q,
) -> std::result::Result<Vec<tokio_postgres::Row>, Attempt> {
    Ok(tx.query(&q.sql, &refs(&q.params)).await?)
}

/// Rows of one `text` column, parsed as JSON.
async fn json_rows(tx: &Transaction<'_>, q: &Q) -> std::result::Result<Vec<Value>, Attempt> {
    query(tx, q)
        .await?
        .iter()
        .map(|r| {
            serde_json::from_str(r.get::<_, &str>(0))
                .map_err(|e| Attempt::Failed(fireweed_dsl::Error::Unavailable(e.to_string())))
        })
        .collect()
}

fn ids_json(ids: &[i64]) -> Value {
    Value::Array(ids.iter().map(|i| json!(i)).collect())
}

pub async fn statement(tx: &Transaction<'_>, sp: &Space, st: &RStatement) -> R {
    match st {
        RStatement::InsertQueue {
            obj: o,
            rows,
            on_conflict,
        } => insert_queue(tx, sp, o, rows, *on_conflict).await,
        RStatement::InsertTable {
            obj: o,
            rows,
            on_conflict,
        } => insert_table(tx, sp, o, rows, *on_conflict).await,
        RStatement::Claim {
            obj: o,
            limit,
            lease_ms,
            worker,
            filter,
            eligible_at,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let (elig, due_bound) = match eligible_at {
                Some(e) => {
                    let e = ob.operand(&mut q, e);
                    (e.clone(), format!("greatest({e}, {NOW})"))
                }
                None => (NOW.to_string(), NOW.to_string()),
            };
            let attempts_left = match ob.max_attempts {
                MaxAttempts::Unlimited => String::new(),
                MaxAttempts::Count(n) => format!(" AND o.attempts < {n}"),
            };
            let filter = filter
                .as_ref()
                .map(|p| format!(" AND {}", ob.pred(&mut q, p)))
                .unwrap_or_default();
            let queue = q.bind(Some(o.name.clone()));
            let desc = if sp.schema.queues[&o.name].priority == fireweed_dsl::PriorityOrder::Desc {
                " DESC"
            } else {
                ""
            };
            let worker = q.bind(Some(worker.clone()));
            let lease = q.duration(*lease_ms);
            let n = q.bind_i64(*limit as i64);
            let item = item_json(sp, &o.name, &ob, true);
            q.sql = format!(
                "WITH c AS (
                   SELECT o.id, o.due_at AS prev_due FROM {t} o
                   WHERE o.state IN ('pending', 'leased') AND o.due_at <= {due_bound}
                     AND ((o.state = 'pending' AND o.due_at <= {elig})
                          OR (o.state = 'leased' AND o.due_at <= {NOW}{attempts_left})){filter}
                     AND NOT EXISTS (SELECT 1 FROM \"{s}\".fw_queue_state p WHERE p.queue = {queue} AND p.paused)
                   ORDER BY o.priority{desc}, o.due_at, o.id
                   LIMIT {n}
                   FOR UPDATE SKIP LOCKED),
                 u AS (
                   UPDATE {t} o SET state = 'leased', lease_token = gen_random_uuid()::text, lease_owner = {worker},
                     due_at = {NOW} + {lease}, attempts = o.attempts + 1, version = o.version + 1, updated_at = {NOW}
                   FROM c WHERE o.id = c.id
                   RETURNING o.priority, c.prev_due, o.id, {item} AS j)
                 SELECT j::text FROM u ORDER BY priority{desc}, prev_due, id",
                t = ob.table,
                s = sp.schema_name,
            );
            Ok(Outcome::Done(Value::Array(json_rows(tx, &q).await?)))
        }
        RStatement::Settle {
            obj: o,
            outcome,
            items,
            after_ms,
            set,
            body,
            require_all,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let exhausted = ob.exhausted("o");
            let (state, due, finished, attempts) = match outcome {
                SettleOutcome::Complete => (
                    "'complete'".to_string(),
                    "o.due_at".to_string(),
                    NOW.to_string(),
                    "o.attempts".to_string(),
                ),
                SettleOutcome::Fail => (
                    "'failed'".into(),
                    "o.due_at".into(),
                    NOW.into(),
                    "o.attempts".into(),
                ),
                SettleOutcome::Retry => (
                    format!("CASE WHEN {exhausted} THEN 'failed' ELSE 'pending' END"),
                    format!("{NOW} + {}", q.duration(*after_ms)),
                    format!("CASE WHEN {exhausted} THEN {NOW} END"),
                    "o.attempts".into(),
                ),
                SettleOutcome::Release => (
                    "'pending'".into(),
                    format!("{NOW} + {}", q.duration(*after_ms)),
                    "NULL".into(),
                    "greatest(o.attempts - 1, 0)".into(),
                ),
            };
            let mut sets = vec![
                format!("state = {state}"),
                format!("due_at = {due}"),
                format!("finished_at = {finished}"),
                format!("attempts = {attempts}"),
            ];
            sets.extend(common_sets(&ob, &mut q, set, body));
            settle_like(tx, &ob, q, sets, items, *require_all).await
        }
        RStatement::Renew {
            obj: o,
            items,
            lease_ms,
            require_all,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let lease = q.duration(*lease_ms);
            let sets = vec![
                format!("due_at = {NOW} + {lease}"),
                "version = o.version + 1".into(),
                format!("updated_at = {NOW}"),
            ];
            settle_like(tx, &ob, q, sets, items, *require_all).await
        }
        RStatement::Update {
            obj: o,
            sel,
            set,
            body,
            leased,
            token,
            limit,
            on_limit,
            after,
            expect,
            override_merge,
        } => {
            let ob = obj(sp, o);
            let mut ids =
                select_ids(tx, &ob, sel, *leased, token.as_deref(), *after, *limit + 1).await?;
            let mut out = json!({});
            if ids.len() as u32 > *limit {
                match on_limit {
                    OnLimit::Reject => return Ok(Outcome::Rejected(RejectReason::LimitExceeded)),
                    OnLimit::Step => {
                        ids.truncate(*limit as usize);
                        out["more"] = json!(true);
                    }
                }
            }
            if *on_limit == OnLimit::Step {
                out["last"] = json!(ids.last());
            }
            out["matched"] = json!(ids.len());
            if expect.is_some_and(|e| !e.holds(ids.len() as u64)) {
                return Ok(Outcome::Rejected(RejectReason::ExpectUnmet));
            }
            if ids.is_empty() {
                return Ok(Outcome::Done(out));
            }
            let mut q = Q::default();
            let mut sets: Vec<String> = common_sets(&ob, &mut q, set, body);
            if o.kind == Kind::Table
                && let Some(ttl) = sp.schema.tables[&o.name].expire_after_write
            {
                sets.push(format!("expires_at = {NOW} + {}", q.duration(ttl.0)));
            }
            if *leased == LeasedPolicy::Break {
                sets.push(format!(
                    "state = CASE WHEN o.state = 'leased' THEN 'pending' ELSE o.state END, \
                     due_at = CASE WHEN o.state = 'leased' THEN {NOW} ELSE o.due_at END, \
                     lease_token = NULL, lease_owner = NULL"
                ));
            }
            // Old values of fields with merge rules, to check after the update.
            let rules: Vec<(&Column, MergeRule)> = if *override_merge || o.kind == Kind::Queue {
                vec![]
            } else {
                let decl = &sp.schema.tables[&o.name];
                set.iter()
                    .filter_map(|(c, _)| {
                        decl.fields
                            .get(&c.name)
                            .and_then(|f| f.merge)
                            .map(|m| (c, m))
                    })
                    .collect()
            };
            let mut olds = vec!["id".to_string()];
            let mut checks = vec!["true".to_string()];
            for (i, (c, m)) in rules.iter().enumerate() {
                let col = Obj::col_name(c);
                olds.push(format!("{col} AS old_{i}"));
                checks.push(match m {
                    MergeRule::AddOnly => {
                        format!("(old.old_{i} IS NULL OR coalesce(o.{col} @> old.old_{i}, false))")
                    }
                    MergeRule::NonDecreasing => {
                        format!("(old.old_{i} IS NULL OR coalesce(o.{col} >= old.old_{i}, false))")
                    }
                });
            }
            let idp = q.bind_json(&ids_json(&ids));
            q.sql = format!(
                "UPDATE {t} o SET {sets}
                 FROM (SELECT {olds} FROM {t} WHERE id = ANY(ARRAY(SELECT jsonb_array_elements_text({idp})::bigint))) old
                 WHERE o.id = old.id
                 RETURNING ({checks})",
                t = ob.table,
                sets = sets.join(", "),
                olds = olds.join(", "),
                checks = checks.join(" AND "),
            );
            let ok = query(tx, &q).await?.iter().all(|r| r.get::<_, bool>(0));
            if !ok {
                return Ok(Outcome::Rejected(RejectReason::MergeViolation));
            }
            Ok(Outcome::Done(out))
        }
        RStatement::Delete {
            obj: o,
            sel,
            limit,
            expect,
        } => {
            let ob = obj(sp, o);
            let ids =
                select_ids(tx, &ob, sel, LeasedPolicy::Preserve, None, None, *limit + 1).await?;
            if ids.len() as u32 > *limit {
                return Ok(Outcome::Rejected(RejectReason::LimitExceeded));
            }
            if expect.is_some_and(|e| !e.holds(ids.len() as u64)) {
                return Ok(Outcome::Rejected(RejectReason::ExpectUnmet));
            }
            if !ids.is_empty() {
                let mut q = Q::default();
                let idp = q.bind_json(&ids_json(&ids));
                q.sql = format!(
                    "DELETE FROM {} WHERE id = ANY(ARRAY(SELECT jsonb_array_elements_text({idp})::bigint))",
                    ob.table
                );
                query(tx, &q).await?;
            }
            Ok(Outcome::Done(json!({"matched": ids.len()})))
        }
        RStatement::Assert {
            obj: o,
            sel,
            expect,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let cond = selection(&ob, &mut q, sel);
            // Count only as far as the expectation needs.
            let cap = expect.max.map_or(expect.min.unwrap_or(0), |m| m + 1).max(1);
            let cap = q.bind_i64(cap as i64);
            q.sql = format!(
                "SELECT count(*) FROM (SELECT 1 FROM {} o WHERE {cond} LIMIT {cap}) x",
                ob.table
            );
            let n: i64 = query(tx, &q).await?[0].get(0);
            if !expect.holds(n as u64) {
                return Ok(Outcome::Rejected(RejectReason::ExpectUnmet));
            }
            Ok(Outcome::Done(json!({"matched": n})))
        }
        RStatement::Select(s) => Ok(Outcome::Done(Value::Array(select(tx, sp, s).await?))),
        RStatement::SetPaused { obj: o, paused } => {
            let mut q = Q::default();
            let name = q.bind(Some(o.name.clone()));
            let p = q.bind(Some(paused.to_string()));
            q.sql = format!(
                "INSERT INTO \"{}\".fw_queue_state (queue, paused) VALUES ({name}, {p}::boolean)
                 ON CONFLICT (queue) DO UPDATE SET paused = EXCLUDED.paused",
                sp.schema_name
            );
            query(tx, &q).await?;
            Ok(Outcome::Done(json!({})))
        }
    }
}

/// `set`, `body`, version and `updated_at` for an update of rows aliased `o`.
fn common_sets(
    ob: &Obj,
    q: &mut Q,
    set: &[(Column, fireweed_dsl::check::Expr)],
    body: &[fireweed_dsl::check::RPatch],
) -> Vec<String> {
    let mut sets: Vec<String> = set.iter().map(|(c, e)| ob.assign(q, c, e)).collect();
    sets.extend(ob.patches(q, body));
    sets.push("version = o.version + 1".into());
    sets.push(format!("updated_at = {NOW}"));
    sets
}

/// `complete`, `fail`, `retry`, `release` and `renew`: one `UPDATE` over the
/// items whose token matches an unexpired lease.
async fn settle_like(
    tx: &Transaction<'_>,
    ob: &Obj,
    mut q: Q,
    mut sets: Vec<String>,
    items: &[(i64, String)],
    require_all: bool,
) -> R {
    if !sets.iter().any(|s| s.starts_with("version")) {
        sets.push("version = o.version + 1".into());
    }
    if !sets.iter().any(|s| s.starts_with("state")) {
        // renew keeps the lease
    } else {
        sets.push("lease_token = NULL".into());
        sets.push("lease_owner = NULL".into());
    }
    let list = Value::Array(
        items
            .iter()
            .map(|(id, t)| json!({"id": id, "token": t}))
            .collect(),
    );
    let items_p = q.bind_json(&list);
    q.sql = format!(
        "UPDATE {t} o SET {sets}
         FROM (SELECT DISTINCT * FROM jsonb_to_recordset({items_p}) AS i(id bigint, token text)) i
         WHERE o.id = i.id AND o.lease_token = i.token AND o.state = 'leased' AND o.due_at > {NOW}
         RETURNING o.id",
        t = ob.table,
        sets = sets.join(", "),
    );
    let done: BTreeSet<i64> = query(tx, &q).await?.iter().map(|r| r.get(0)).collect();
    if require_all && items.iter().any(|(id, _)| !done.contains(id)) {
        return Ok(Outcome::Rejected(RejectReason::StaleLease));
    }
    Ok(Outcome::Done(Value::Array(
        items
            .iter()
            .map(|(id, _)| json!({"id": id, "status": if done.contains(id) { "ok" } else { "stale" }}))
            .collect(),
    )))
}

/// The `WHERE` condition for a selection, over rows aliased `o`.
fn selection(ob: &Obj, q: &mut Q, sel: &Selection) -> String {
    let mut conds = vec![ob.live()];
    if let Some(p) = &sel.filter {
        conds.push(ob.pred(q, p));
    }
    if let Some(ids) = &sel.ids {
        let p = q.bind_json(&ids_json(ids));
        conds.push(format!(
            "o.id = ANY(ARRAY(SELECT jsonb_array_elements_text({p})::bigint))"
        ));
    }
    if let Some(keys) = &sel.queue_keys {
        let p = q.bind_json(&Value::Array(keys.iter().map(|k| json!(k)).collect()));
        conds.push(format!(
            "o.key = ANY(ARRAY(SELECT jsonb_array_elements_text({p})))"
        ));
    }
    if let Some(keys) = &sel.table_keys {
        // Key columns come from the first key's operands (all name key fields).
        let tuples: Vec<String> = keys
            .iter()
            .map(|k| {
                format!(
                    "({})",
                    k.iter()
                        .map(|v| ob.operand(q, v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect();
        let cols: Vec<String> = ob.key.iter().map(|k| format!("o.\"f_{k}\"")).collect();
        conds.push(format!("({}) IN ({})", cols.join(", "), tuples.join(", ")));
    }
    conds.join(" AND ")
}

/// Ids of the rows a write or guard selects, locked, in id order.
async fn select_ids(
    tx: &Transaction<'_>,
    ob: &Obj,
    sel: &Selection,
    leased: LeasedPolicy,
    token: Option<&str>,
    after: Option<i64>,
    limit: u32,
) -> std::result::Result<Vec<i64>, Attempt> {
    let mut q = Q::default();
    let mut cond = selection(ob, &mut q, sel);
    if ob.kind == Kind::Queue {
        let held = format!("(o.state = 'leased' AND o.due_at > {NOW})");
        if let Some(t) = token {
            let t = q.bind(Some(t.to_owned()));
            cond.push_str(&format!(" AND {held} AND o.lease_token = {t}"));
        } else if leased == LeasedPolicy::Skip {
            cond.push_str(&format!(" AND NOT {held}"));
        }
    }
    if let Some(a) = after {
        cond.push_str(&format!(" AND o.id > {}", q.bind_i64(a)));
    }
    let n = q.bind_i64(limit as i64);
    q.sql = format!(
        "SELECT o.id FROM {} o WHERE {cond} ORDER BY o.id LIMIT {n} FOR UPDATE",
        ob.table
    );
    Ok(query(tx, &q).await?.iter().map(|r| r.get(0)).collect())
}

/// The JSON a claim or select returns for a queue item.
fn item_json(sp: &Space, queue: &str, ob: &Obj, token: bool) -> String {
    let mut cols: Vec<(String, String)> = vec![
        ("id".into(), "o.id".into()),
        ("key".into(), "o.key".into()),
        ("attempts".into(), "o.attempts".into()),
        ("priority".into(), "o.priority".into()),
        ("version".into(), "o.version".into()),
        ("body".into(), "o.body".into()),
    ];
    if token {
        cols.push(("token".into(), "o.lease_token".into()));
    }
    for f in sp.schema.queues[queue].fields.keys() {
        cols.push((f.clone(), format!("o.\"f_{f}\"")));
    }
    ob.json_of(&cols)
}

async fn select(
    tx: &Transaction<'_>,
    sp: &Space,
    s: &RSelect,
) -> std::result::Result<Vec<Value>, Attempt> {
    let ob = obj(sp, &s.obj);
    let mut q = Q::default();
    let mut cond = vec![ob.live()];
    if let Some(p) = &s.filter {
        cond.push(ob.pred(&mut q, p));
    }
    let mut named: BTreeMap<String, String> = BTreeMap::new();
    let (cols, group) = match &s.output {
        Output::Fields(cs) => {
            let cols: Vec<(String, String)> =
                cs.iter().map(|c| (c.name.clone(), ob.col(c))).collect();
            (cols, String::new())
        }
        Output::Aggregate { aggs, group_by } => {
            let mut cols: Vec<(String, String)> = group_by
                .iter()
                .map(|c| (c.name.clone(), ob.col(c)))
                .collect();
            for (name, a) in aggs {
                let e = match a {
                    Agg::Count => "count(*)".to_string(),
                    Agg::Sum(c) => format!("sum({})", ob.col(c)),
                    Agg::Min(c) => format!("min({})", ob.col(c)),
                    Agg::Max(c) => format!("max({})", ob.col(c)),
                    Agg::Avg(c) => format!("avg({})", ob.col(c)),
                    Agg::Percentile(c, p) => format!(
                        "percentile_disc({}::double precision) WITHIN GROUP (ORDER BY {})",
                        q.bind(Some(p.to_string())),
                        ob.col(c)
                    ),
                };
                cols.push((name.clone(), e));
            }
            let group = if group_by.is_empty() {
                String::new()
            } else {
                format!(
                    " GROUP BY {}",
                    group_by
                        .iter()
                        .map(|c| ob.col(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            (cols, group)
        }
    };
    for (k, v) in &cols {
        named.insert(k.clone(), v.clone());
    }
    let order = if s.order_by.is_empty() {
        String::new()
    } else {
        format!(
            " ORDER BY {}",
            s.order_by
                .iter()
                .map(|(n, desc)| format!("{}{}", named[n], if *desc { " DESC" } else { "" }))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let n = q.bind_i64(s.limit as i64);
    q.sql = format!(
        "SELECT {}::text FROM {} o WHERE {}{group}{order} LIMIT {n}",
        ob.json_of(&cols),
        ob.table,
        cond.join(" AND ")
    );
    json_rows(tx, &q).await
}

async fn insert_queue(
    tx: &Transaction<'_>,
    sp: &Space,
    o: &DslObj,
    rows: &[fireweed_dsl::check::QueueRow],
    on_conflict: OnConflict,
) -> R {
    let ob = obj(sp, o);
    let decl = &sp.schema.queues[&o.name];
    let mut q = Q::default();
    let mut recs = vec![];
    for r in rows {
        let mut m = serde_json::Map::new();
        m.insert("key".into(), json!(r.key));
        m.insert("priority".into(), json!(r.priority));
        m.insert("due".into(), json!(r.due));
        m.insert("after_ms".into(), json!(r.after_ms));
        m.insert("body".into(), r.body.clone().unwrap_or(Value::Null));
        m.insert("ord".into(), json!(recs.len() + 1));
        for (k, v) in &r.fields {
            m.insert(format!("f_{k}"), v.clone());
        }
        recs.push(Value::Object(m));
    }
    let retention = q.duration(decl.retention.0);
    let keys = q.bind_json(&Value::Array(
        rows.iter()
            .filter_map(|r| r.key.clone())
            .map(Value::from)
            .collect(),
    ));
    // Finished items past retention release their keys now, not at the next sweep.
    q.sql = format!(
        "DELETE FROM {t} o WHERE o.key = ANY(ARRAY(SELECT jsonb_array_elements_text({keys}))) AND o.finished_at <= {NOW} - {retention}",
        t = ob.table
    );
    query(tx, &q).await?;

    let mut q = Q::default();
    let p = q.bind_json(&Value::Array(recs));
    let mut defs = vec![
        "key text".to_string(),
        "priority bigint".into(),
        "due timestamptz".into(),
        "after_ms bigint".into(),
        "body jsonb".into(),
    ];
    let mut cols = vec![];
    let mut vals = vec![];
    for (f, d) in &decl.fields {
        defs.push(format!("\"f_{f}\" {}", record_type(d.ty)));
        cols.push(format!("\"f_{f}\""));
        vals.push(record_value(&format!("r.\"f_{f}\""), d.ty));
    }
    let extra_cols = if cols.is_empty() {
        String::new()
    } else {
        format!(", {}", cols.join(", "))
    };
    let extra_vals = if vals.is_empty() {
        String::new()
    } else {
        format!(", {}", vals.join(", "))
    };
    q.sql = format!(
        "WITH r AS (
           SELECT r.*, nextval(pg_get_serial_sequence('{t}', 'id')) AS new_id
           FROM jsonb_to_recordset({p}) AS r({defs}, ord bigint)),
         ins AS (
           INSERT INTO {t} (id, key, state, priority, due_at, attempts, version, created_at, updated_at, body{extra_cols})
           SELECT r.new_id, r.key, 'pending', coalesce(r.priority, 0),
                  coalesce(r.due, {NOW} + coalesce(r.after_ms, 0) * interval '1 millisecond'),
                  0, 1, {NOW}, {NOW}, r.body{extra_vals}
           FROM r ORDER BY r.ord
           ON CONFLICT (key) DO NOTHING
           RETURNING id)
         SELECT r.ord, r.key, ins.id FROM r LEFT JOIN ins ON ins.id = r.new_id ORDER BY r.ord",
        t = ob.table,
        defs = defs.join(", "),
    );
    let res = query(tx, &q).await?;
    let mut out = vec![];
    let mut missing = vec![];
    for row in &res {
        let key: Option<String> = row.get(1);
        let id: Option<i64> = row.get(2);
        match id {
            Some(id) => out.push(json!({"id": id, "key": key, "status": "inserted"})),
            None => {
                missing.push(key.clone().expect("only keyed rows conflict"));
                out.push(json!({"id": null, "key": key, "status": "exists"}));
            }
        }
    }
    if !missing.is_empty() {
        if on_conflict == OnConflict::Reject {
            return Ok(Outcome::Rejected(RejectReason::KeyConflict));
        }
        let mut q = Q::default();
        let ks = q.bind_json(&Value::Array(
            missing.into_iter().map(Value::from).collect(),
        ));
        q.sql = format!(
            "SELECT key, id FROM {} WHERE key = ANY(ARRAY(SELECT jsonb_array_elements_text({ks})))",
            ob.table
        );
        let found: BTreeMap<String, i64> = query(tx, &q)
            .await?
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        for e in &mut out {
            if e["id"].is_null()
                && let Some(k) = e["key"].as_str()
            {
                e["id"] = json!(found.get(k));
            }
        }
    }
    Ok(Outcome::Done(Value::Array(out)))
}

/// How a field travels through `jsonb_to_recordset`.
fn record_type(t: FieldType) -> &'static str {
    match t {
        FieldType::TextSet => "jsonb",
        other => crate::lower::cast(other),
    }
}

fn record_value(e: &str, t: FieldType) -> String {
    match t {
        FieldType::TextSet => format!("fireweed.text_set({e})"),
        _ => e.to_string(),
    }
}

async fn insert_table(
    tx: &Transaction<'_>,
    sp: &Space,
    o: &DslObj,
    rows: &[BTreeMap<String, Value>],
    on_conflict: OnConflict,
) -> R {
    let ob = obj(sp, o);
    let decl = &sp.schema.tables[&o.name];
    let mut q = Q::default();
    let mut defs = vec![];
    let mut cols = vec![];
    let mut vals = vec![];
    for (f, d) in &decl.fields {
        defs.push(format!("\"f_{f}\" {}", record_type(d.ty)));
        cols.push(format!("\"f_{f}\""));
        vals.push(record_value(&format!("r.\"f_{f}\""), d.ty));
    }
    let recs: Vec<Value> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut m: serde_json::Map<String, Value> = r
                .iter()
                .map(|(k, v)| (format!("f_{k}"), v.clone()))
                .collect();
            m.insert("ord".into(), json!(i + 1));
            Value::Object(m)
        })
        .collect();
    let expires = match decl.expire_after_write {
        Some(d) => format!("{NOW} + {}", q.duration(d.0)),
        None => "NULL::timestamptz".into(),
    };
    let p = q.bind_json(&Value::Array(recs));
    let t = &ob.table;
    let (defs, cols, vals) = (defs.join(", "), cols.join(", "), vals.join(", "));
    if decl.sequenced {
        let name = q.bind(Some(o.name.clone()));
        let n = q.bind_i64(rows.len() as i64);
        q.sql = format!(
            "WITH s AS (UPDATE \"{s}\".fw_sequences SET last = last + {n} WHERE tbl = {name} RETURNING last),
             r AS (SELECT * FROM jsonb_to_recordset({p}) AS r({defs}, ord bigint))
             INSERT INTO {t} (id, version, created_at, updated_at, expires_at, {cols})
             SELECT (SELECT last FROM s) - {n} + r.ord, 1, {NOW}, {NOW}, {expires}, {vals} FROM r ORDER BY r.ord
             RETURNING id",
            s = sp.schema_name,
        );
        let mut ids: Vec<i64> = query(tx, &q).await?.iter().map(|r| r.get(0)).collect();
        ids.sort_unstable();
        return Ok(Outcome::Done(Value::Array(
            ids.into_iter()
                .map(|id| json!({"id": id, "status": "inserted"}))
                .collect(),
        )));
    }
    let key: Vec<String> = decl.key.iter().map(|k| format!("\"f_{k}\"")).collect();
    let key = key.join(", ");
    let rkey: Vec<String> = decl.key.iter().map(|k| format!("r.\"f_{k}\"")).collect();
    let rkey = rkey.join(", ");
    // Rows past their expiry release their keys now.
    if decl.expire_after_write.is_some() {
        let mut d = Q::default();
        let p = d.bind_json(&Value::Array(
            rows.iter()
                .map(|r| {
                    Value::Object(
                        r.iter()
                            .map(|(k, v)| (format!("f_{k}"), v.clone()))
                            .collect(),
                    )
                })
                .collect(),
        ));
        d.sql = format!(
            "DELETE FROM {t} o USING jsonb_to_recordset({p}) AS r({defs})
             WHERE ({key_o}) = ({rkey}) AND o.expires_at <= {NOW}",
            key_o = decl
                .key
                .iter()
                .map(|k| format!("o.\"f_{k}\""))
                .collect::<Vec<_>>()
                .join(", "),
        );
        query(tx, &d).await?;
    }
    q.sql = format!(
        "WITH r AS (
           SELECT r.*, nextval(pg_get_serial_sequence('{t}', 'id')) AS new_id
           FROM jsonb_to_recordset({p}) AS r({defs}, ord bigint)),
         ins AS (
           INSERT INTO {t} (id, version, created_at, updated_at, expires_at, {cols})
           SELECT r.new_id, 1, {NOW}, {NOW}, {expires}, {vals} FROM r ORDER BY r.ord
           ON CONFLICT ({key}) DO NOTHING
           RETURNING id)
         SELECT r.ord, ins.id, (SELECT jsonb_agg(x) FROM jsonb_array_elements(jsonb_build_array({rkey_json})) x)::text
         FROM r LEFT JOIN ins ON ins.id = r.new_id ORDER BY r.ord",
        rkey_json = decl.key.iter().map(|k| format!("to_jsonb(r.\"f_{k}\")")).collect::<Vec<_>>().join(", "),
    );
    let res = query(tx, &q).await?;
    let mut out = vec![];
    let mut conflicts = vec![];
    for (i, row) in res.iter().enumerate() {
        match row.get::<_, Option<i64>>(1) {
            Some(id) => out.push(json!({"id": id, "status": "inserted"})),
            None => {
                conflicts.push(i);
                out.push(json!({"id": null, "status": "exists"}));
            }
        }
    }
    if conflicts.is_empty() {
        return Ok(Outcome::Done(Value::Array(out)));
    }
    if on_conflict == OnConflict::Reject {
        return Ok(Outcome::Rejected(RejectReason::KeyConflict));
    }
    // Look up (and, with `update`, update) each conflicting row by its key.
    for i in conflicts {
        let row = &rows[i];
        let mut q = Q::default();
        let conds: Vec<String> = decl
            .key
            .iter()
            .map(|k| format!("o.\"f_{k}\" = {}", q.literal(&row[k], decl.fields[k].ty)))
            .collect();
        let found = if on_conflict == OnConflict::Update {
            let mut sets = vec![];
            let mut olds = vec!["id".to_string()];
            let mut checks = vec!["true".to_string()];
            for (j, (k, v)) in row
                .iter()
                .filter(|(k, _)| !decl.key.contains(*k))
                .enumerate()
            {
                let f = &decl.fields[k];
                sets.push(format!("\"f_{k}\" = {}", q.literal(v, f.ty)));
                if let Some(m) = f.merge {
                    olds.push(format!("\"f_{k}\" AS old_{j}"));
                    checks.push(match m {
                        MergeRule::AddOnly => format!(
                            "(old.old_{j} IS NULL OR coalesce(o.\"f_{k}\" @> old.old_{j}, false))"
                        ),
                        MergeRule::NonDecreasing => format!(
                            "(old.old_{j} IS NULL OR coalesce(o.\"f_{k}\" >= old.old_{j}, false))"
                        ),
                    });
                }
            }
            sets.push("version = o.version + 1".into());
            sets.push(format!("updated_at = {NOW}"));
            sets.push(format!("expires_at = {expires}"));
            q.sql = format!(
                "UPDATE {t} o SET {} FROM (SELECT {} FROM {t} o WHERE {}) old WHERE o.id = old.id RETURNING o.id, ({})",
                sets.join(", "),
                olds.join(", "),
                conds.join(" AND "),
                checks.join(" AND "),
            );
            let r = query(tx, &q).await?;
            if r.iter().any(|r| !r.get::<_, bool>(1)) {
                return Ok(Outcome::Rejected(RejectReason::MergeViolation));
            }
            r.first().map(|r| r.get::<_, i64>(0))
        } else {
            q.sql = format!("SELECT o.id FROM {t} o WHERE {}", conds.join(" AND "));
            query(tx, &q).await?.first().map(|r| r.get::<_, i64>(0))
        };
        out[i] = json!({
            "id": found,
            "status": if on_conflict == OnConflict::Update { "updated" } else { "exists" },
        });
    }
    Ok(Outcome::Done(Value::Array(out)))
}

pub async fn sweep(tx: &Transaction<'_>, sp: &Space, batch: i64) -> Result<Swept> {
    let mut out = Swept::default();
    let n = batch.to_string();
    for (name, decl) in &sp.schema.queues {
        let t = queue_table(&sp.schema_name, name);
        let exhausted = match decl.max_attempts {
            MaxAttempts::Unlimited => "false".to_string(),
            MaxAttempts::Count(m) => format!("o.attempts >= {m}"),
        };
        out.expired_leases += tx
            .execute(
                &format!(
                    "UPDATE {t} o SET state = CASE WHEN {exhausted} THEN 'failed' ELSE 'pending' END,
                       finished_at = CASE WHEN {exhausted} THEN o.due_at END,
                       lease_token = NULL, lease_owner = NULL
                     WHERE o.id IN (SELECT id FROM {t} WHERE state = 'leased' AND due_at <= {NOW} LIMIT $1::text::bigint)"
                ),
                &[&n],
            )
            .await
            .map_err(db)?;
        out.deleted += tx
            .execute(
                &format!(
                    "DELETE FROM {t} WHERE id IN (SELECT id FROM {t} WHERE finished_at <= {NOW} - $2::text::bigint * interval '1 millisecond' LIMIT $1::text::bigint)"
                ),
                &[&n, &decl.retention.0.to_string()],
            )
            .await
            .map_err(db)?;
    }
    for (name, decl) in &sp.schema.tables {
        if decl.expire_after_write.is_none() {
            continue;
        }
        let t = table_table(&sp.schema_name, name);
        out.deleted += tx
            .execute(
                &format!("DELETE FROM {t} WHERE id IN (SELECT id FROM {t} WHERE expires_at <= {NOW} LIMIT $1::text::bigint)"),
                &[&n],
            )
            .await
            .map_err(db)?;
    }
    let r = format!("\"{}\".fw_requests", sp.schema_name);
    out.deleted += tx
        .execute(
            &format!(
                "DELETE FROM {r} WHERE request_id IN (SELECT request_id FROM {r} WHERE recorded_at <= {NOW} - $2::text::bigint * interval '1 millisecond' LIMIT $1::text::bigint)"
            ),
            &[&n, &sp.schema.request_retention.0.to_string()],
        )
        .await
        .map_err(db)?;
    Ok(out)
}
