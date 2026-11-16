//! Each resolved statement as one SQL statement (API-006 §Statements).

use std::collections::{BTreeMap, BTreeSet};

use fireweed_dsl::check::{
    Agg, Column, Expr, Kind, MAX_SCAN, Obj as DslObj, Output, QueueRow, RPatch, RSelect,
    RStatement, Selection,
};
use fireweed_dsl::{
    Expect, FieldType, LeasedPolicy, MaxAttempts, MergeRule, OnConflict, OnLimit,
    Outcome as Settle, RejectReason,
};
use serde_json::{Value, json};
use tokio_postgres::{Row, Transaction};

use crate::ddl::{queue_table, table_table};
use crate::lower::{NOW, Obj, Q, cast, merge_ok};
use crate::{Attempt, Space, Swept, rows};

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

async fn query(tx: &Transaction<'_>, q: &Q) -> std::result::Result<Vec<Row>, Attempt> {
    Ok(rows(tx, q).await?)
}

fn parse(text: &str) -> std::result::Result<Value, Attempt> {
    serde_json::from_str(text)
        .map_err(|e| Attempt::Failed(fireweed_dsl::Error::Unavailable(e.to_string())))
}

/// `min <= count <= max` in SQL, for a count expression.
fn expect_sql(q: &mut Q, count: &str, e: &Option<Expect>) -> String {
    let Some(e) = e else { return "true".into() };
    let mut parts = vec!["true".to_string()];
    if let Some(m) = e.min {
        parts.push(format!("{count} >= {}", q.bind_i64(m as i64)));
    }
    if let Some(m) = e.max {
        parts.push(format!("{count} <= {}", q.bind_i64(m as i64)));
    }
    parts.join(" AND ")
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
            let (elig, bound) = match eligible_at {
                Some(e) => {
                    let e = ob.operand(&mut q, e);
                    (e.clone(), format!("greatest({e}, {NOW})"))
                }
                None => (NOW.to_string(), NOW.to_string()),
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
            let item = item_json(sp, &o.name, &ob);
            // Claim order is due time, then priority, then id: the claim
            // index's order, so the scan stops after `limit` claimable items
            // whether most items are due or most are sleeping.
            q.sql = format!(
                "WITH c AS (
                   SELECT o.id, o.due_at AS prev_due, o.priority FROM {t} o
                   WHERE o.state IN ('pending', 'leased') AND o.due_at <= {bound}
                     AND ((o.state = 'pending' AND o.due_at <= {elig})
                          OR (o.state = 'leased' AND o.due_at <= {NOW} AND NOT ({exhausted}))){filter}
                     AND NOT EXISTS (SELECT 1 FROM \"{s}\".fw_queue_state p WHERE p.queue = {queue} AND p.paused)
                   ORDER BY o.due_at, o.priority{desc}, o.id
                   LIMIT {n}
                   FOR UPDATE SKIP LOCKED),
                 u AS (
                   UPDATE {t} o SET state = 'leased', lease_token = gen_random_uuid()::text, lease_owner = {worker},
                     due_at = {NOW} + {lease}, attempts = o.attempts + 1, version = o.version + 1, updated_at = {NOW}
                   FROM c WHERE o.id = c.id
                   RETURNING c.prev_due, c.priority, o.id, {item} AS j)
                 SELECT j::text FROM u ORDER BY prev_due, priority{desc}, id",
                t = ob.table,
                s = sp.schema_name,
                exhausted = ob.exhausted("o"),
            );
            let items = query(tx, &q)
                .await?
                .iter()
                .map(|r| parse(r.get(0)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Outcome::Done(Value::Array(items)))
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
            let mut sets = match outcome {
                Settle::Complete => {
                    vec!["state = 'complete'".into(), format!("finished_at = {NOW}")]
                }
                Settle::Fail => vec!["state = 'failed'".into(), format!("finished_at = {NOW}")],
                Settle::Retry => vec![
                    format!("state = CASE WHEN {exhausted} THEN 'failed' ELSE 'pending' END"),
                    format!("finished_at = CASE WHEN {exhausted} THEN {NOW} END"),
                    format!("due_at = {NOW} + {}", q.duration(*after_ms)),
                ],
                Settle::Release => vec![
                    "state = 'pending'".into(),
                    format!("due_at = {NOW} + {}", q.duration(*after_ms)),
                    "attempts = greatest(o.attempts - 1, 0)".into(),
                ],
            };
            sets.push("lease_token = NULL".into());
            sets.push("lease_owner = NULL".into());
            lease_update(tx, &ob, q, sets, set, body, items, *require_all).await
        }
        RStatement::Renew {
            obj: o,
            items,
            lease_ms,
            require_all,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let sets = vec![format!("due_at = {NOW} + {}", q.duration(*lease_ms))];
            lease_update(tx, &ob, q, sets, &[], &[], items, *require_all).await
        }
        RStatement::Update {
            obj: o,
            sel,
            set,
            body,
            leased,
            limit,
            on_limit,
            expect,
            override_merge,
        } => {
            update(
                tx,
                sp,
                o,
                sel,
                set,
                body,
                *leased,
                *limit,
                *on_limit,
                expect,
                *override_merge,
            )
            .await
        }
        RStatement::Delete {
            obj: o,
            sel,
            limit,
            expect,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let cond = selection(&ob, &mut q, sel);
            let lim = q.bind_i64(*limit as i64);
            let lim1 = q.bind_i64(*limit as i64 + 1);
            let ok = expect_sql(&mut q, "(SELECT c FROM total)", expect);
            q.sql = format!(
                "WITH m AS (SELECT o.id FROM {t} o WHERE {cond} ORDER BY o.id LIMIT {lim1} FOR UPDATE),
                 total AS (SELECT count(*) AS c FROM m),
                 d AS (DELETE FROM {t} o USING m WHERE o.id = m.id AND (SELECT c FROM total) <= {lim} AND {ok} RETURNING 1)
                 SELECT (SELECT c FROM total), (SELECT count(*) FROM d)",
                t = ob.table
            );
            let total: i64 = query(tx, &q).await?[0].get(0);
            if total > *limit as i64 {
                return Ok(Outcome::Rejected(RejectReason::LimitExceeded));
            }
            if expect.is_some_and(|e| !e.holds(total as u64)) {
                return Ok(Outcome::Rejected(RejectReason::ExpectUnmet));
            }
            Ok(Outcome::Done(json!({"matched": total})))
        }
        RStatement::Assert {
            obj: o,
            sel,
            expect,
        } => {
            let ob = obj(sp, o);
            let mut q = Q::default();
            let cond = selection(&ob, &mut q, sel);
            // Count only as far as the expectation needs. Matched rows stay
            // locked against change until commit.
            let cap =
                q.bind_i64(expect.max.map_or(expect.min.unwrap_or(0), |m| m + 1).max(1) as i64);
            q.sql = format!(
                "SELECT count(*) FROM (SELECT 1 FROM {} o WHERE {cond} LIMIT {cap} FOR KEY SHARE) x",
                ob.table
            );
            let n: i64 = query(tx, &q).await?[0].get(0);
            if !expect.holds(n as u64) {
                return Ok(Outcome::Rejected(RejectReason::ExpectUnmet));
            }
            Ok(Outcome::Done(json!({"matched": n})))
        }
        RStatement::Select(s) => select(tx, sp, s).await,
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

/// `set`, `body`, version and `updated_at` for rows aliased `o`.
fn common_sets(ob: &Obj, q: &mut Q, set: &[(Column, Expr)], body: &[RPatch]) -> Vec<String> {
    let mut sets: Vec<String> = set.iter().map(|(c, e)| ob.assign(q, c, e)).collect();
    sets.extend(ob.patches(q, body));
    sets.push("version = o.version + 1".into());
    sets.push(format!("updated_at = {NOW}"));
    sets
}

/// Settle and renew: one `UPDATE` over the items whose token matches an
/// unexpired lease. Other items are reported `stale` (ADR-025 D6).
#[allow(clippy::too_many_arguments)]
async fn lease_update(
    tx: &Transaction<'_>,
    ob: &Obj,
    mut q: Q,
    mut sets: Vec<String>,
    set: &[(Column, Expr)],
    body: &[RPatch],
    items: &[(i64, String)],
    require_all: bool,
) -> R {
    sets.extend(common_sets(ob, &mut q, set, body));
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
        conds.push(format!("o.id = ANY({})", q.bind_ids(ids)));
    }
    if let Some(keys) = &sel.queue_keys {
        let p = q.bind_json(&Value::Array(keys.iter().map(|k| json!(k)).collect()));
        conds.push(format!(
            "o.key = ANY(ARRAY(SELECT jsonb_array_elements_text({p})))"
        ));
    }
    if let Some(keys) = &sel.table_keys {
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

/// One statement: lock up to `limit + 1` matching rows in id order, count
/// them, and update the first `limit` if the count allows it.
#[allow(clippy::too_many_arguments)]
async fn update(
    tx: &Transaction<'_>,
    sp: &Space,
    o: &DslObj,
    sel: &Selection,
    set: &[(Column, Expr)],
    body: &[RPatch],
    leased: LeasedPolicy,
    limit: u32,
    on_limit: OnLimit,
    expect: &Option<Expect>,
    override_merge: bool,
) -> R {
    let ob = obj(sp, o);
    let mut q = Q::default();
    let mut cond = selection(&ob, &mut q, sel);
    if ob.kind == Kind::Queue && leased == LeasedPolicy::Skip {
        cond.push_str(&format!(
            " AND NOT (o.state = 'leased' AND o.due_at > {NOW})"
        ));
    }
    // Fields with merge rules: their old values travel with the locked rows.
    let rules: Vec<(String, MergeRule)> = match (ob.kind, override_merge) {
        (Kind::Table, false) => {
            let decl = &sp.schema.tables[&o.name];
            set.iter()
                .filter_map(|(c, _)| {
                    decl.fields
                        .get(&c.name)
                        .and_then(|f| f.merge)
                        .map(|m| (Obj::col_name(c), m))
                })
                .collect()
        }
        _ => vec![],
    };
    let olds: String = rules
        .iter()
        .enumerate()
        .map(|(i, (c, _))| format!(", o.{c} AS old_{i}"))
        .collect();
    let mut checks: Vec<String> = rules
        .iter()
        .enumerate()
        .map(|(i, (c, m))| merge_ok(*m, &format!("old.old_{i}"), &format!("o.{c}")))
        .collect();
    checks.push("true".into());

    let mut sets = common_sets(&ob, &mut q, set, body);
    let sets_state = set.iter().any(|(c, _)| c.system && c.name == "state");
    let sets_due = set.iter().any(|(c, _)| c.system && c.name == "due_at");
    if sets_state {
        // `o.state` here is the old value; the new one decides finished_at.
        let new_state = set
            .iter()
            .find(|(c, _)| c.system && c.name == "state")
            .map(|(_, e)| e);
        if let Some(Expr::Operand(fireweed_dsl::check::Operand::Literal(Value::String(st), _))) =
            new_state
        {
            let fin = if st == "pending" {
                "NULL".to_string()
            } else {
                NOW.to_string()
            };
            sets.push(format!("finished_at = {fin}"));
        }
    }
    if sets_state || leased == LeasedPolicy::Break {
        sets.push("lease_token = NULL".into());
        sets.push("lease_owner = NULL".into());
    }
    if leased == LeasedPolicy::Break {
        if !sets_state {
            sets.push(
                "state = CASE WHEN o.state = 'leased' THEN 'pending' ELSE o.state END".into(),
            );
        }
        if !sets_due {
            sets.push(format!(
                "due_at = CASE WHEN o.state = 'leased' THEN {NOW} ELSE o.due_at END"
            ));
        }
    }
    if ob.kind == Kind::Table
        && let Some(ttl) = sp.schema.tables[&o.name].expire_after_write
    {
        sets.push(format!("expires_at = {NOW} + {}", q.duration(ttl.0)));
    }
    let lim = q.bind_i64(limit as i64);
    let lim1 = q.bind_i64(limit as i64 + 1);
    let mut go = expect_sql(&mut q, "(SELECT c FROM total)", expect);
    if on_limit == OnLimit::Reject {
        go = format!("{go} AND (SELECT c FROM total) <= {lim}");
    }
    q.sql = format!(
        "WITH m AS (SELECT o.id{olds} FROM {t} o WHERE {cond} ORDER BY o.id LIMIT {lim1} FOR UPDATE),
         total AS (SELECT count(*) AS c FROM m),
         take AS (SELECT * FROM m ORDER BY id LIMIT {lim}),
         u AS (UPDATE {t} o SET {sets} FROM take old WHERE o.id = old.id AND {go} RETURNING {checks} AS ok)
         SELECT (SELECT c FROM total), (SELECT max(id) FROM take), coalesce((SELECT bool_and(ok) FROM u), true)",
        t = ob.table,
        sets = sets.join(", "),
        checks = checks.join(" AND "),
    );
    let r = &query(tx, &q).await?[0];
    let (total, last, ok): (i64, Option<i64>, bool) = (r.get(0), r.get(1), r.get(2));
    let over = total > limit as i64;
    if over && on_limit == OnLimit::Reject {
        return Ok(Outcome::Rejected(RejectReason::LimitExceeded));
    }
    if expect.is_some_and(|e| !e.holds(total as u64)) {
        return Ok(Outcome::Rejected(RejectReason::ExpectUnmet));
    }
    if !ok {
        return Ok(Outcome::Rejected(RejectReason::MergeViolation));
    }
    let mut out = json!({"matched": total.min(limit as i64)});
    if on_limit == OnLimit::Step {
        out["last"] = json!(last);
        if over {
            out["more"] = json!(true);
        }
    }
    Ok(Outcome::Done(out))
}

/// The JSON a claim returns for a queue item, with its lease token.
fn item_json(sp: &Space, queue: &str, ob: &Obj) -> String {
    let mut cols: Vec<(String, String)> = ["id", "key", "attempts", "priority", "version", "body"]
        .iter()
        .map(|c| (c.to_string(), format!("o.{c}")))
        .collect();
    cols.push(("token".into(), "o.lease_token".into()));
    for f in sp.schema.queues[queue].fields.keys() {
        cols.push((f.clone(), format!("o.\"f_{f}\"")));
    }
    ob.json_of(&cols)
}

/// A select. Without `order_by` or an aggregate, its `limit` bounds the rows
/// read. Otherwise it reads through a CTE capped at `MAX_SCAN + 1` rows and
/// rejects with `limit_exceeded` past the cap.
async fn select(tx: &Transaction<'_>, sp: &Space, s: &RSelect) -> R {
    let ob = obj(sp, &s.obj);
    let mut q = Q::default();
    let mut cond = vec![ob.live()];
    if let Some(p) = &s.filter {
        cond.push(ob.pred(&mut q, p));
    }
    let (cols, group) = match &s.output {
        Output::Fields(cs) => (
            cs.iter()
                .map(|c| (c.name.clone(), ob.col(c)))
                .collect::<Vec<_>>(),
            String::new(),
        ),
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
    let named: BTreeMap<String, String> = cols.iter().cloned().collect();
    let order = if s.order_by.is_empty() {
        String::new()
    } else {
        let keys: Vec<String> = s
            .order_by
            .iter()
            .map(|(n, desc)| format!("{}{}", named[n], if *desc { " DESC" } else { "" }))
            .collect();
        format!(" ORDER BY {}", keys.join(", "))
    };
    let n = q.bind_i64(s.limit as i64);
    let bounded = matches!(s.output, Output::Fields(_)) && s.order_by.is_empty();
    let json = ob.json_of(&cols);
    let cond = cond.join(" AND ");
    if bounded {
        q.sql = format!(
            "SELECT {json}::text, 0::bigint FROM {} o WHERE {cond} LIMIT {n}",
            ob.table
        );
    } else {
        let cap = q.bind_i64(MAX_SCAN as i64 + 1);
        q.sql = format!(
            "WITH o AS (SELECT * FROM {t} o WHERE {cond} LIMIT {cap})
             SELECT {json}::text, (SELECT count(*) FROM o) FROM o{group}{order} LIMIT {n}",
            t = ob.table
        );
    }
    let rs = query(tx, &q).await?;
    if rs
        .first()
        .is_some_and(|r| r.get::<_, i64>(1) > MAX_SCAN as i64)
    {
        return Ok(Outcome::Rejected(RejectReason::LimitExceeded));
    }
    let out = rs
        .iter()
        .map(|r| parse(r.get(0)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Outcome::Done(Value::Array(out)))
}

/// How a field travels through `jsonb_to_recordset`.
fn record_type(t: FieldType) -> &'static str {
    match t {
        FieldType::TextSet => "jsonb",
        other => cast(other),
    }
}

fn record_value(e: &str, t: FieldType) -> String {
    match t {
        FieldType::TextSet => format!("fireweed.text_set({e})"),
        _ => e.to_string(),
    }
}

/// One statement: insert the rows; a key held by a finished item past
/// retention is taken over; other conflicts are reported `exists`.
async fn insert_queue(
    tx: &Transaction<'_>,
    sp: &Space,
    o: &DslObj,
    rows: &[QueueRow],
    on_conflict: OnConflict,
) -> R {
    let ob = obj(sp, o);
    let decl = &sp.schema.queues[&o.name];
    let mut q = Q::default();
    let recs: Vec<Value> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut m = serde_json::Map::new();
            m.insert("ord".into(), json!(i + 1));
            m.insert("key".into(), json!(r.key));
            m.insert("priority".into(), json!(r.priority));
            m.insert("due".into(), json!(r.due));
            m.insert("after_ms".into(), json!(r.after_ms));
            m.insert("body".into(), r.body.clone().unwrap_or(Value::Null));
            for (k, v) in &r.fields {
                m.insert(format!("f_{k}"), v.clone());
            }
            Value::Object(m)
        })
        .collect();
    let p = q.bind_json(&Value::Array(recs));
    let retention = q.duration(decl.retention.0);
    let mut defs: Vec<String> = [
        "ord bigint",
        "key text",
        "priority bigint",
        "due timestamptz",
        "after_ms bigint",
        "body jsonb",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let mut cols = vec![];
    let mut vals = vec![];
    for (f, d) in &decl.fields {
        defs.push(format!("\"f_{f}\" {}", record_type(d.ty)));
        cols.push(format!("\"f_{f}\""));
        vals.push(record_value(&format!("r.\"f_{f}\""), d.ty));
    }
    let system = [
        "id",
        "state",
        "priority",
        "due_at",
        "attempts",
        "version",
        "lease_owner",
        "lease_token",
        "created_at",
        "updated_at",
        "finished_at",
        "body",
    ];
    let take_over: Vec<String> = system
        .iter()
        .map(|c| c.to_string())
        .chain(cols.iter().cloned())
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect();
    let cols: String = cols.iter().map(|c| format!(", {c}")).collect();
    let vals: String = vals.iter().map(|v| format!(", {v}")).collect();
    q.sql = format!(
        "WITH r AS (
           SELECT r.*, nextval(pg_get_serial_sequence('{t}', 'id')) AS new_id
           FROM jsonb_to_recordset({p}) AS r({defs})),
         ins AS (
           INSERT INTO {t} AS t (id, key, state, priority, due_at, attempts, version, lease_owner, lease_token, created_at, updated_at, finished_at, body{cols})
           SELECT r.new_id, r.key, 'pending', coalesce(r.priority, 0),
                  coalesce(r.due, {NOW} + coalesce(r.after_ms, 0) * interval '1 millisecond'),
                  0, 1, NULL, NULL, {NOW}, {NOW}, NULL, r.body{vals}
           FROM r ORDER BY r.ord
           ON CONFLICT (key) DO UPDATE SET {take_over}
             WHERE t.finished_at <= {NOW} - {retention}
           RETURNING t.id)
         SELECT r.key, coalesce(ins.id, e.id), ins.id IS NOT NULL
         FROM r LEFT JOIN ins ON ins.id = r.new_id LEFT JOIN {t} e ON e.key = r.key
         ORDER BY r.ord",
        t = ob.table,
        defs = defs.join(", "),
        take_over = take_over.join(", "),
    );
    let mut out = vec![];
    for row in query(tx, &q).await? {
        let (key, id, inserted): (Option<String>, Option<i64>, bool) =
            (row.get(0), row.get(1), row.get(2));
        if !inserted && on_conflict == OnConflict::Reject {
            return Ok(Outcome::Rejected(RejectReason::KeyConflict));
        }
        out.push(
            json!({"id": id, "key": key, "status": if inserted { "inserted" } else { "exists" }}),
        );
    }
    Ok(Outcome::Done(Value::Array(out)))
}

/// One statement per insert. A row past its expiry gives up its key; with
/// `update`, a conflicting row takes the given fields under merge rules.
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
    let mut defs = vec!["ord bigint".to_string()];
    let mut cols = vec![];
    let mut vals = vec![];
    for (f, d) in &decl.fields {
        defs.push(format!("\"f_{f}\" {}", record_type(d.ty)));
        cols.push(format!("\"f_{f}\""));
        vals.push(record_value(&format!("r.\"f_{f}\""), d.ty));
    }
    let defs = defs.join(", ");
    if decl.sequenced {
        // The counter row is held to commit, so ids are gapless and in commit
        // order; the checker puts these inserts last in a program.
        let name = q.bind(Some(o.name.clone()));
        let n = q.bind_i64(rows.len() as i64);
        q.sql = format!(
            "WITH s AS (UPDATE \"{s}\".fw_sequences SET last = last + {n} WHERE tbl = {name} RETURNING last),
             r AS (SELECT * FROM jsonb_to_recordset({p}) AS r({defs}))
             INSERT INTO {t} (id, version, created_at, updated_at, expires_at, {cols})
             SELECT (SELECT last FROM s) - {n} + r.ord, 1, {NOW}, {NOW}, {expires}, {vals} FROM r ORDER BY r.ord
             RETURNING id",
            s = sp.schema_name,
            cols = cols.join(", "),
            vals = vals.join(", "),
        );
        let mut ids: Vec<i64> = query(tx, &q).await?.iter().map(|r| r.get(0)).collect();
        ids.sort_unstable();
        return Ok(Outcome::Done(Value::Array(
            ids.into_iter()
                .map(|id| json!({"id": id, "status": "inserted"}))
                .collect(),
        )));
    }
    let expired = format!("coalesce(t.expires_at <= {NOW}, false)");
    let conflict = match on_conflict {
        OnConflict::Update => {
            let given: Vec<&String> = rows[0].keys().filter(|k| !decl.key.contains(*k)).collect();
            let mut sets: Vec<String> = given
                .iter()
                .map(|g| format!("\"f_{g}\" = EXCLUDED.\"f_{g}\""))
                .collect();
            if decl.expire_after_write.is_some() {
                // An expired row starts over: fields this insert doesn't give are cleared.
                for f in decl
                    .fields
                    .keys()
                    .filter(|f| !decl.key.contains(*f) && !given.contains(f))
                {
                    sets.push(format!(
                        "\"f_{f}\" = CASE WHEN {expired} THEN NULL ELSE t.\"f_{f}\" END"
                    ));
                }
            }
            sets.push(format!(
                "version = CASE WHEN {expired} THEN 1 ELSE t.version + 1 END"
            ));
            sets.push("updated_at = EXCLUDED.updated_at".into());
            sets.push("expires_at = EXCLUDED.expires_at".into());
            let rules: Vec<String> = given
                .iter()
                .filter_map(|g| {
                    decl.fields[*g].merge.map(|m| {
                        format!(
                            "({expired} OR {})",
                            merge_ok(m, &format!("t.\"f_{g}\""), &format!("EXCLUDED.\"f_{g}\""))
                        )
                    })
                })
                .collect();
            let guard = if rules.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", rules.join(" AND "))
            };
            format!("DO UPDATE SET {}{guard}", sets.join(", "))
        }
        _ if decl.expire_after_write.is_some() => {
            let take_over: Vec<String> =
                ["id", "version", "created_at", "updated_at", "expires_at"]
                    .iter()
                    .map(|c| c.to_string())
                    .chain(cols.iter().cloned())
                    .map(|c| format!("{c} = EXCLUDED.{c}"))
                    .collect();
            format!("DO UPDATE SET {} WHERE {expired}", take_over.join(", "))
        }
        _ => "DO NOTHING".into(),
    };
    let key: Vec<String> = decl.key.iter().map(|k| format!("\"f_{k}\"")).collect();
    let on = |a: &str| {
        decl.key
            .iter()
            .map(|k| format!("{a}.\"f_{k}\" = r.\"f_{k}\""))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    q.sql = format!(
        "WITH r AS (
           SELECT r.*, nextval(pg_get_serial_sequence('{t}', 'id')) AS new_id
           FROM jsonb_to_recordset({p}) AS r({defs})),
         ins AS (
           INSERT INTO {t} AS t (id, version, created_at, updated_at, expires_at, {cols})
           SELECT r.new_id, 1, {NOW}, {NOW}, {expires}, {vals} FROM r ORDER BY r.ord
           ON CONFLICT ({key}) {conflict}
           RETURNING t.id, t.xmax = 0 AS fresh, {key})
         SELECT coalesce(ins.id, e.id), ins.id IS NOT NULL, coalesce(ins.fresh, false)
         FROM r LEFT JOIN ins ON {on_ins} LEFT JOIN {t} e ON {on_e}
         ORDER BY r.ord",
        cols = cols.join(", "),
        vals = vals.join(", "),
        key = key.join(", "),
        on_ins = on("ins"),
        on_e = on("e"),
    );
    let mut out = vec![];
    for row in query(tx, &q).await? {
        let (id, returned, fresh): (Option<i64>, bool, bool) = (row.get(0), row.get(1), row.get(2));
        let status = match (returned, on_conflict) {
            (true, OnConflict::Update) if !fresh => "updated",
            (true, _) => "inserted",
            (false, OnConflict::Update) => {
                return Ok(Outcome::Rejected(RejectReason::MergeViolation));
            }
            (false, OnConflict::Reject) => return Ok(Outcome::Rejected(RejectReason::KeyConflict)),
            (false, OnConflict::Skip) => "exists",
        };
        out.push(json!({"id": id, "status": status}));
    }
    Ok(Outcome::Done(Value::Array(out)))
}

/// Writes back expired leases and deletes what retention expired, at most
/// `batch` rows per object. Rows other transactions hold are skipped, and
/// each row's condition is checked again under its lock.
pub async fn sweep(
    tx: &Transaction<'_>,
    sp: &Space,
    batch: i64,
) -> std::result::Result<Swept, tokio_postgres::Error> {
    let mut out = Swept::default();
    // (counts as expired leases, SQL, the retention bound as $2 if any)
    let mut work: Vec<(bool, String, Option<i64>)> = vec![];
    let pick = |t: &str, cond: &str, id: &str| {
        format!(
            "(SELECT {id} FROM {t} o WHERE {cond} LIMIT $1::text::bigint FOR UPDATE SKIP LOCKED)"
        )
    };
    let aged =
        |col: &str| format!("o.{col} <= {NOW} - $2::text::bigint * interval '1 millisecond'");
    for (name, decl) in &sp.schema.queues {
        let t = queue_table(&sp.schema_name, name);
        let ob = Obj {
            table: t.clone(),
            kind: Kind::Queue,
            max_attempts: decl.max_attempts,
            key: vec![],
        };
        let exhausted = ob.exhausted("o");
        let expired = format!("o.state = 'leased' AND o.due_at <= {NOW}");
        work.push((
            true,
            format!(
                "UPDATE {t} o SET state = CASE WHEN {exhausted} THEN 'failed' ELSE 'pending' END,
                   finished_at = CASE WHEN {exhausted} THEN o.due_at END,
                   lease_token = NULL, lease_owner = NULL, version = o.version + 1
                 FROM {s} s WHERE o.id = s.id AND {expired}",
                s = pick(&t, &expired, "id"),
            ),
            None,
        ));
        let done = aged("finished_at");
        work.push((
            false,
            format!(
                "DELETE FROM {t} o USING {s} s WHERE o.id = s.id AND {done}",
                s = pick(&t, &done, "id")
            ),
            Some(decl.retention.0),
        ));
    }
    for (name, decl) in &sp.schema.tables {
        if decl.expire_after_write.is_none() {
            continue;
        }
        let t = table_table(&sp.schema_name, name);
        let gone = format!("o.expires_at <= {NOW}");
        work.push((
            false,
            format!(
                "DELETE FROM {t} o USING {s} s WHERE o.id = s.id AND {gone}",
                s = pick(&t, &gone, "id")
            ),
            None,
        ));
    }
    let r = format!("\"{}\".fw_requests", sp.schema_name);
    let old = aged("recorded_at");
    work.push((
        false,
        format!(
            "DELETE FROM {r} o USING {s} s WHERE o.request_id = s.request_id AND {old}",
            s = pick(&r, &old, "request_id")
        ),
        Some(sp.schema.request_retention.0),
    ));
    for (leases, sql, bound) in work {
        let mut params = vec![Some(batch.to_string())];
        params.extend(bound.map(|b| Some(b.to_string())));
        let q = Q {
            sql: format!("WITH x AS ({sql} RETURNING 1) SELECT count(*) FROM x"),
            params,
        };
        let n = rows(tx, &q).await?[0].get::<_, i64>(0) as u64;
        if leases {
            out.expired_leases += n;
        } else {
            out.deleted += n;
        }
    }
    Ok(out)
}
