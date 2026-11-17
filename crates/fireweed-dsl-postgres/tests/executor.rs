//! Runs DSL documents against PostgreSQL (API-008 §Conformance). Needs
//! `FIREWEED_PG_TEST_URL`; each test uses its own space.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_dsl::{Error, Program, RejectReason, Response, Schema, Status, from_yaml};
use fireweed_dsl_postgres::{apply_schema, execute, execute_batch, get_schema, sweep};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Option<Client> {
    let url = std::env::var("FIREWEED_PG_TEST_URL").ok()?;
    let (c, conn) = tokio_postgres::connect(&url, NoTls).await.expect("connect");
    tokio::spawn(conn);
    Some(c)
}

fn space(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{tag}-{}-{nanos}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

const SCHEMA: &str = r#"
version: 1
queues:
  work:
    max_attempts: 2
    fields:
      campaign: {type: text, index: true}
      awaits:   {type: text_set, index: true}
      step:     {type: int}
      email:    {type: text}
      entity:   {type: text, index: true}
tables:
  slots:
    key: [contact, hour]
    expire_after_write: 7d
    fields:
      contact: {type: text}
      hour:    {type: timestamp}
      count:   {type: int, merge: non_decreasing}
  supp:
    key: [address]
    fields:
      address: {type: text}
      reasons: {type: text_set, merge: add_only}
  timeline:
    sequenced: true
    fields:
      instance: {type: int, index: true}
      event:    {type: text}
reads:
  - name: awaiting
    version: 1
    params: {wait: text}
    select:
      from: work
      where: {contains: [awaits, {param: wait}]}
      fields: [id, key]
      order_by: [{field: id}]
      limit: 100
"#;

async fn setup(tag: &str) -> Option<(Client, String)> {
    let mut c = connect().await?;
    let sp = space(tag);
    let s: Schema = from_yaml(SCHEMA).unwrap();
    apply_schema(&mut c, "t", &sp, &s, None).await.unwrap();
    Some((c, sp))
}

async fn run(c: &mut Client, sp: &str, doc: &str) -> Response {
    try_run(c, sp, doc)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{doc}"))
}

async fn try_run(c: &mut Client, sp: &str, doc: &str) -> Result<Response, Error> {
    let p = program(c, sp, doc).await;
    execute(c, "t", sp, &p).await
}

/// Parses a program. A request id without `issued_at` gets one the first
/// time it is seen, from the database clock, and the same one after: the
/// retry rule a caller follows (API-008 §Request ids).
async fn program(c: &Client, sp: &str, doc: &str) -> Program {
    static ISSUED: Mutex<BTreeMap<(String, String), String>> = Mutex::new(BTreeMap::new());
    let mut p: Program = from_yaml(doc).unwrap();
    if let (Some(rid), None) = (&p.request_id, &p.issued_at) {
        let k = (sp.to_owned(), rid.clone());
        let known = ISSUED.lock().unwrap().get(&k).cloned();
        let at = match known {
            Some(at) => at,
            None => {
                let at: String = c
                    .query_one("SELECT fireweed.now()::text", &[])
                    .await
                    .unwrap()
                    .get(0);
                ISSUED.lock().unwrap().insert(k, at.clone());
                at
            }
        };
        p.issued_at = Some(at);
    }
    p
}

async fn set_now(c: &Client, at: &str) {
    c.batch_execute(&format!("SET fireweed.now = '{at}'"))
        .await
        .unwrap();
}

fn rows(r: &Response, i: usize) -> Vec<Value> {
    r.results[i].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn schema_creates_tables_and_indexes_and_only_adds() {
    let Some((mut c, sp)) = setup("ddl").await else {
        return;
    };
    let schema_name: String = c
        .query_one(
            "SELECT 'fws_' || id FROM fireweed.spaces WHERE tenant = 't' AND space = $1",
            &[&sp],
        )
        .await
        .unwrap()
        .get(0);
    let idx: Vec<String> = c
        .query(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 ORDER BY indexdef",
            &[&schema_name],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let has = |s: &str| idx.iter().any(|d| d.contains(s));
    assert!(
        has("q_work USING btree (due_at, priority, id) WHERE (state = ANY"),
        "{idx:#?}"
    );
    assert!(has("q_work USING gin (f_awaits)"), "{idx:#?}");
    assert!(
        has("UNIQUE INDEX") && has("t_slots USING btree (f_contact, f_hour)"),
        "{idx:#?}"
    );

    let mut s: Schema = from_yaml(SCHEMA).unwrap();
    assert!(
        !apply_schema(&mut c, "t", &sp, &s, Some(1))
            .await
            .unwrap()
            .changed
    );
    s.version = 2;
    s.queues.get_mut("work").unwrap().fields.insert(
        "region".into(),
        from_yaml("{type: text, index: true}").unwrap(),
    );
    assert_eq!(
        apply_schema(&mut c, "t", &sp, &s, Some(5)).await,
        Err(Error::SchemaConflict { current: Some(1) })
    );
    // An index on a live object would block its writes while it builds.
    assert!(matches!(
        apply_schema(&mut c, "t", &sp, &s, Some(1)).await,
        Err(Error::Invalid(_))
    ));
    s.queues
        .get_mut("work")
        .unwrap()
        .fields
        .insert("region".into(), from_yaml("{type: text}").unwrap());
    assert!(
        apply_schema(&mut c, "t", &sp, &s, Some(1))
            .await
            .unwrap()
            .changed
    );
    assert_eq!(get_schema(&c, "t", &sp).await.unwrap().unwrap().version, 2);
    // Removing a field is refused.
    s.version = 3;
    s.queues.get_mut("work").unwrap().fields.remove("step");
    assert!(matches!(
        apply_schema(&mut c, "t", &sp, &s, Some(2)).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn enqueue_claim_settle() {
    let Some((mut c, sp)) = setup("queue").await else {
        return;
    };
    let r = run(
        &mut c,
        &sp,
        r#"
request_id: e1
statements:
  - insert:
      into: work
      rows:
        - {key: a, priority: 2, campaign: c1, body: {n: 1}}
        - {key: b, priority: 1, campaign: c1}
        - {key: c, priority: 3, campaign: c2, after: 1h}
  - insert: {into: work, rows: [{key: a, priority: 9}]}
"#,
    )
    .await;
    let ins = rows(&r, 0);
    assert_eq!(
        ins.iter()
            .map(|e| e["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["inserted", "inserted", "inserted"]
    );
    assert_eq!(rows(&r, 1)[0]["status"], "exists");
    assert_eq!(rows(&r, 1)[0]["id"], ins[0]["id"]);

    let r = run(
        &mut c,
        &sp,
        "request_id: c1\nstatements:\n  - claim: {from: work, limit: 5, lease: 30s, worker: w1}\n",
    )
    .await;
    let got = rows(&r, 0);
    // c is not due; b (priority 1) before a (priority 2).
    assert_eq!(
        got.iter()
            .map(|i| i["key"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["b", "a"]
    );
    assert_eq!(got[1]["body"], json!({"n": 1}));
    assert_eq!(got[0]["attempts"], json!(1));
    let (b, tok_b) = (
        got[0]["id"].clone(),
        got[0]["token"].as_str().unwrap().to_owned(),
    );
    let a = got[1]["id"].clone();

    let r = run(&mut c, &sp, &format!(r#"
request_id: s1
statements:
  - settle: {{queue: work, outcome: complete, items: [{{id: {b}, token: "{tok_b}"}}, {{id: {a}, token: wrong}}], set: {{step: 5}}}}
"#)).await;
    assert_eq!(
        rows(&r, 0),
        vec![
            json!({"id": b, "status": "ok"}),
            json!({"id": a, "status": "stale"})
        ]
    );

    let r = run(
        &mut c,
        &sp,
        r#"
statements:
  - select:
      from: work
      where: {in: [campaign, [c1, c2]]}
      aggregate: {n: count}
      group_by: [state]
      order_by: [{field: state}]
      limit: 10
  - select: {from: work, where: {eq: [key, b]}, fields: [state, step, version], limit: 1}
"#,
    )
    .await;
    assert_eq!(
        rows(&r, 0),
        vec![
            json!({"state": "complete", "n": 1}),
            json!({"state": "leased", "n": 1}),
            json!({"state": "pending", "n": 1}),
        ]
    );
    // Inserted, claimed, then settled: three writes, three new versions
    // from the space's sequence, so the version is past the three inserts'.
    let row = &rows(&r, 1)[0];
    assert_eq!(
        (&row["state"], &row["step"]),
        (&json!("complete"), &json!(5))
    );
    assert!(row["version"].as_i64().unwrap() > 3, "{row}");

    // A paused queue claims nothing.
    run(
        &mut c,
        &sp,
        "request_id: p1\nstatements:\n  - pause: {queue: work}\n",
    )
    .await;
    let r = run(&mut c, &sp, "request_id: c2\nstatements:\n  - claim: {from: work, limit: 5, lease: 30s, worker: w1, eligible_at: \"2100-01-01T00:00:00Z\"}\n").await;
    assert!(rows(&r, 0).is_empty());
    run(
        &mut c,
        &sp,
        "request_id: p2\nstatements:\n  - resume: {queue: work}\n",
    )
    .await;
    // eligible_at makes the delayed item due; the leased one stays leased.
    let r = run(&mut c, &sp, "request_id: c3\nstatements:\n  - claim: {from: work, limit: 5, lease: 30s, worker: w1, eligible_at: \"2100-01-01T00:00:00Z\"}\n").await;
    assert_eq!(
        rows(&r, 0)
            .iter()
            .map(|i| i["key"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["c"]
    );
}

#[tokio::test]
async fn concurrent_claims_never_lease_an_item_twice() {
    let Some((mut c, sp)) = setup("race").await else {
        return;
    };
    let items: Vec<String> = (0..200)
        .map(|i| format!("{{key: k{i}, priority: {}}}", i % 7))
        .collect();
    run(
        &mut c,
        &sp,
        &format!(
            "request_id: fill\nstatements:\n  - insert: {{into: work, rows: [{}]}}\n",
            items.join(", ")
        ),
    )
    .await;
    let mut tasks = vec![];
    for w in 0..4 {
        let sp = sp.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = connect().await.unwrap();
            let mut got = vec![];
            for n in 0..40 {
                let r = run(&mut c, &sp, &format!("request_id: w{w}-{n}\nstatements:\n  - claim: {{from: work, limit: 3, lease: 60s, worker: w{w}}}\n")).await;
                got.extend(rows(&r, 0).into_iter().map(|i| i["id"].as_i64().unwrap()));
            }
            got
        }));
    }
    let mut all = vec![];
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let distinct: std::collections::BTreeSet<_> = all.iter().collect();
    assert_eq!(all.len(), 200, "every item claimed once");
    assert_eq!(distinct.len(), 200, "an item was leased twice");
}

#[tokio::test]
async fn lease_expiry_reclaims_then_fails_at_max_attempts() {
    let Some((mut c, sp)) = setup("expiry").await else {
        return;
    };
    set_now(&c, "2030-01-01T00:00:00Z").await;
    run(
        &mut c,
        &sp,
        "request_id: e\nstatements:\n  - insert: {into: work, rows: [{key: x, campaign: c}]}\n",
    )
    .await;
    let claim = |n: u32| {
        format!(
            "request_id: c{n}\nstatements:\n  - claim: {{from: work, limit: 1, lease: 10s, worker: w}}\n"
        )
    };
    let r = run(&mut c, &sp, &claim(1)).await;
    let first = rows(&r, 0)[0].clone();
    // Still leased: nothing to claim.
    assert!(rows(&run(&mut c, &sp, &claim(2)).await, 0).is_empty());
    set_now(&c, "2030-01-01T00:00:11Z").await;
    // The renew comes too late: the lease expired.
    let r = run(&mut c, &sp, &format!(
        "request_id: r1\nstatements:\n  - renew: {{queue: work, items: [{{id: {}, token: \"{}\"}}], lease: 10s}}\n",
        first["id"], first["token"].as_str().unwrap()
    )).await;
    assert_eq!(rows(&r, 0)[0]["status"], "stale");
    let r = run(&mut c, &sp, &claim(3)).await;
    assert_eq!(rows(&r, 0)[0]["attempts"], json!(2));
    set_now(&c, "2030-01-01T00:00:30Z").await;
    // Attempts used up: the item counts as failed and is not claimed.
    assert!(rows(&run(&mut c, &sp, &claim(4)).await, 0).is_empty());
    let state =
        "statements:\n  - select: {from: work, where: {eq: [key, x]}, fields: [state], limit: 1}\n";
    assert_eq!(
        rows(&run(&mut c, &sp, state).await, 0),
        vec![json!({"state": "failed"})]
    );
    let swept = sweep(&mut c, "t", &sp, 100).await.unwrap();
    assert_eq!(swept.expired_leases, 1);
    assert_eq!(
        rows(&run(&mut c, &sp, state).await, 0),
        vec![json!({"state": "failed"})]
    );
}

#[tokio::test]
async fn replay_conflict_and_recorded_rejection() {
    let Some((mut c, sp)) = setup("replay").await else {
        return;
    };
    let doc = "request_id: r1\nstatements:\n  - insert: {into: work, rows: [{key: a}]}\n";
    let first = run(&mut c, &sp, doc).await;
    let again = run(&mut c, &sp, doc).await;
    assert!(again.replayed);
    assert_eq!(again.results, first.results);
    let other = "request_id: r1\nstatements:\n  - insert: {into: work, rows: [{key: b}]}\n";
    assert_eq!(
        try_run(&mut c, &sp, other).await,
        Err(Error::RequestIdConflict)
    );

    // A rejection rolls back the inserts before it, and is recorded.
    let rej = r#"
request_id: r2
statements:
  - insert: {into: work, rows: [{key: new1}]}
  - insert: {into: work, rows: [{key: a}], on_conflict: reject}
"#;
    let r = run(&mut c, &sp, rej).await;
    assert_eq!(r.status, Status::Rejected);
    assert_eq!(
        r.rejected.as_ref().unwrap().reason,
        RejectReason::KeyConflict
    );
    assert_eq!(r.rejected.as_ref().unwrap().statement, 1);
    let replay = run(&mut c, &sp, rej).await;
    assert!(replay.replayed && replay.status == Status::Rejected);
    let n = run(&mut c, &sp, "statements:\n  - select: {from: work, where: {eq: [key, new1]}, aggregate: {n: count}, limit: 1}\n").await;
    assert_eq!(rows(&n, 0), vec![json!({"n": 0})]);
}

#[tokio::test]
async fn counters_merge_rules_and_steps() {
    let Some((mut c, sp)) = setup("tables").await else {
        return;
    };
    let bump = |n: u32| {
        format!(
            r#"
request_id: b{n}
statements:
  - insert: {{into: slots, rows: [{{contact: c1, hour: "2030-01-01T10:00:00Z", count: 0}}]}}
  - update:
      target: slots
      keys: [{{contact: c1, hour: "2030-01-01T10:00:00Z"}}]
      where: {{lt: [count, 2]}}
      set: {{count: {{add: [{{field: count}}, 1]}}}}
      expect: {{min: 1, max: 1}}
"#
        )
    };
    assert_eq!(run(&mut c, &sp, &bump(1)).await.status, Status::Committed);
    assert_eq!(run(&mut c, &sp, &bump(2)).await.status, Status::Committed);
    let third = run(&mut c, &sp, &bump(3)).await;
    assert_eq!(third.rejected.unwrap().reason, RejectReason::ExpectUnmet);

    // non_decreasing: lowering the count rejects unless overridden.
    let lower = |id: &str, ov: bool| {
        format!(
            r#"
request_id: {id}
statements:
  - update: {{target: slots, keys: [{{contact: c1, hour: "2030-01-01T10:00:00Z"}}], set: {{count: 0}}, override_merge: {ov}}}
"#
        )
    };
    assert_eq!(
        run(&mut c, &sp, &lower("l1", false))
            .await
            .rejected
            .unwrap()
            .reason,
        RejectReason::MergeViolation
    );
    assert_eq!(
        run(&mut c, &sp, &lower("l2", true)).await.status,
        Status::Committed
    );

    // add_only sets: set_add works; set_remove rejects.
    let r = run(
        &mut c,
        &sp,
        r#"
request_id: s1
statements:
  - insert: {into: supp, rows: [{address: x, reasons: [bounce]}]}
  - update: {target: supp, keys: [{address: x}], set: {reasons: {set_add: [stop, bounce]}}}
  - select: {from: supp, where: {eq: [address, x]}, fields: [reasons], limit: 1}
"#,
    )
    .await;
    assert_eq!(rows(&r, 2), vec![json!({"reasons": ["bounce", "stop"]})]);
    let r = run(&mut c, &sp, "request_id: s2\nstatements:\n  - update: {target: supp, keys: [{address: x}], set: {reasons: {set_remove: [stop]}}}\n").await;
    assert_eq!(r.rejected.unwrap().reason, RejectReason::MergeViolation);
    // A send's guard: the address is suppressed, so the program rejects.
    let r = run(
        &mut c,
        &sp,
        r#"
request_id: send1
statements:
  - assert: {target: supp, keys: [{address: x}], expect: {max: 0}}
  - insert: {into: work, rows: [{key: send-x}]}
"#,
    )
    .await;
    assert_eq!(r.rejected.unwrap().reason, RejectReason::ExpectUnmet);

    // Stepped bulk update over 25 items, 10 at a time.
    let items: Vec<String> = (0..25)
        .map(|i| format!("{{key: s{i}, campaign: bulk}}"))
        .collect();
    run(
        &mut c,
        &sp,
        &format!(
            "request_id: fill\nstatements:\n  - insert: {{into: work, rows: [{}]}}\n",
            items.join(", ")
        ),
    )
    .await;
    let (mut after, mut steps, mut total) = (None::<i64>, 0, 0);
    loop {
        let a = after
            .map(|a| format!(", {{gt: [id, {a}]}}"))
            .unwrap_or_default();
        let r = run(&mut c, &sp, &format!(
            "request_id: step{steps}\nstatements:\n  - update: {{target: work, where: {{and: [{{eq: [campaign, bulk]}}{a}]}}, set: {{step: 1}}, limit: 10, on_limit: step}}\n"
        )).await;
        let out = &r.results[0];
        total += out["matched"].as_u64().unwrap();
        steps += 1;
        if out.get("more").is_none() {
            break;
        }
        after = out["last"].as_i64();
    }
    assert_eq!((steps, total), (3, 25));
    // Without step, the same update rejects at the limit.
    let r = run(&mut c, &sp, "request_id: big\nstatements:\n  - update: {target: work, where: {eq: [campaign, bulk]}, set: {step: 2}, limit: 10}\n").await;
    assert_eq!(r.rejected.unwrap().reason, RejectReason::LimitExceeded);
}

#[tokio::test]
async fn sequenced_ids_body_patches_and_reads() {
    let Some((mut c, sp)) = setup("misc").await else {
        return;
    };
    let r = run(
        &mut c,
        &sp,
        r#"
request_id: t1
statements:
  - insert: {into: timeline, rows: [{instance: 1, event: a}, {instance: 1, event: b}]}
  - insert: {into: timeline, rows: [{instance: 2, event: c}]}
"#,
    )
    .await;
    let ids: Vec<i64> = [rows(&r, 0), rows(&r, 1)]
        .concat()
        .iter()
        .map(|e| e["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [1, 2, 3]);

    let r = run(
        &mut c,
        &sp,
        r#"
request_id: i1
statements:
  - insert: {into: work, rows: [{key: inst, awaits: [evt-1, evt-2], body: {audit: []}}]}
  - claim: {from: work, limit: 1, lease: 30s, worker: w}
"#,
    )
    .await;
    let it = rows(&r, 1)[0].clone();
    let patch = format!(
        r#"
request_id: i2
statements:
  - settle:
      outcome: release
      queue: work
      items: [{{id: {}, token: "{}"}}]
      set: {{awaits: {{set_remove: [evt-1]}}}}
      body:
        - {{set: /node/name, value: welcome}}
        - {{append: /audit, value: 1, keep_last: 2}}
        - {{append: /audit, value: 2, keep_last: 2}}
        - {{append: /audit, value: 3, keep_last: 2}}
      require_all: true
"#,
        it["id"],
        it["token"].as_str().unwrap()
    );
    assert_eq!(run(&mut c, &sp, &patch).await.status, Status::Committed);
    let r = run(&mut c, &sp, r#"
statements:
  - select: {from: work, where: {eq: [key, inst]}, fields: [body, attempts, state, awaits], limit: 1}
  - read: {name: awaiting, version: 1, params: {wait: evt-2}}
  - read: {name: awaiting, version: 1, params: {wait: evt-1}}
"#).await;
    assert_eq!(
        rows(&r, 0),
        vec![
            json!({"body": {"audit": [2, 3], "node": {"name": "welcome"}}, "attempts": 0, "state": "pending", "awaits": ["evt-2"]})
        ]
    );
    assert_eq!(rows(&r, 1).len(), 1);
    assert!(rows(&r, 2).is_empty());
}

#[tokio::test]
async fn programs_that_break_the_rules_are_refused() {
    let Some((mut c, sp)) = setup("refuse").await else {
        return;
    };
    let bad = [
        // No index on step.
        "statements:\n  - select: {from: work, where: {eq: [step, 1]}, fields: [id], limit: 1}\n",
        // A write without a request id.
        "statements:\n  - insert: {into: work, rows: [{key: a}]}\n",
        // A system column can't be set.
        "request_id: x\nstatements:\n  - update: {target: work, ids: [1], set: {state: done}}\n",
        // Type mismatch.
        "statements:\n  - select: {from: work, where: {eq: [campaign, 3]}, fields: [id], limit: 1}\n",
        // Unknown field.
        "statements:\n  - select: {from: work, where: {eq: [nope, a]}, fields: [id], limit: 1}\n",
        // due_at's only index is partial, so it isn't an access term.
        "statements:\n  - select: {from: work, where: {lt: [due_at, {now: {}}]}, fields: [id], limit: 1}\n",
        // A range alone can match everything.
        "statements:\n  - select: {from: work, where: {ge: [id, 0]}, fields: [id], limit: 1}\n",
        // Comparing a column with itself isn't an access term.
        "statements:\n  - select: {from: work, where: {eq: [campaign, {field: campaign}]}, fields: [id], limit: 1}\n",
        // A claim filter without an access term.
        "request_id: x\nstatements:\n  - claim: {from: work, limit: 1, lease: 30s, worker: w, where: {eq: [step, 1]}}\n",
        // A lookup must use an index of the other object (email isn't indexed).
        "statements:\n  - select: {from: work, where: {and: [{eq: [campaign, c]}, {exists: {in: work, match: {email: x}}}]}, fields: [id], limit: 1}\n",
        // A key twice in one insert.
        "request_id: x\nstatements:\n  - insert: {into: work, rows: [{key: a}, {key: a}]}\n",
        // A write after a sequenced insert.
        "request_id: x\nstatements:\n  - insert: {into: timeline, rows: [{instance: 1, event: a}]}\n  - insert: {into: work, rows: [{key: a}]}\n",
    ];
    for doc in bad {
        assert!(
            matches!(try_run(&mut c, &sp, doc).await, Err(Error::Invalid(_))),
            "{doc}"
        );
    }
    assert_eq!(
        try_run(
            &mut c,
            "no-such-space",
            "statements:\n  - pause: {queue: work}\n"
        )
        .await,
        Err(Error::SpaceNotFound)
    );
}

/// Claims, indexed selects and keyed updates never scan a whole table
/// (ADR-027 D41): PostgreSQL's per-table count of sequential scans doesn't
/// move while they run over 5,000 items.
#[tokio::test]
async fn statements_use_indexes_not_table_scans() {
    let Some((mut c, sp)) = setup("index").await else {
        return;
    };
    for b in 0..5 {
        let items: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "{{key: k{b}-{i}, campaign: c{}, priority: {}}}",
                    i % 50,
                    i % 9
                )
            })
            .collect();
        run(
            &mut c,
            &sp,
            &format!(
                "request_id: f{b}\nstatements:\n  - insert: {{into: work, rows: [{}]}}\n",
                items.join(", ")
            ),
        )
        .await;
    }
    let table: String = c
        .query_one(
            "SELECT 'fws_' || id FROM fireweed.spaces WHERE tenant = 't' AND space = $1",
            &[&sp],
        )
        .await
        .unwrap()
        .get(0);
    c.batch_execute(&format!("ANALYZE \"{table}\".q_work"))
        .await
        .unwrap();
    let before = seq_scans(&c, &table).await;
    run(
        &mut c,
        &sp,
        r#"
request_id: x1
statements:
  - claim: {from: work, limit: 10, lease: 30s, worker: w}
  - claim: {from: work, limit: 5, lease: 30s, worker: w, where: {eq: [campaign, c9]}}
  - select: {from: work, where: {eq: [campaign, c7]}, aggregate: {n: count}, limit: 1}
  - select: {from: work, where: {eq: [campaign, c8]}, aggregate: {n: count}, group_by: [state], limit: 5}
  - update: {target: work, keys: [k1-5, k2-6], set: {step: 1}, leased: preserve}
  - select: {from: work, where: {eq: [key, k3-3]}, fields: [id, state], limit: 1}
"#,
    )
    .await;
    // Leases expire; the sweep writes them back through the state index.
    c.batch_execute("SET fireweed.now = '2100-01-01T00:00:00Z'")
        .await
        .unwrap();
    let swept = sweep(&mut c, "t", &sp, 100).await.unwrap();
    assert_eq!(swept.expired_leases, 15);
    c.batch_execute("RESET fireweed.now").await.unwrap();
    assert_eq!(
        seq_scans(&c, &table).await,
        before,
        "a statement scanned the whole queue table"
    );
}

async fn seq_scans(c: &Client, schema: &str) -> i64 {
    c.batch_execute("SELECT pg_stat_force_next_flush(); SELECT pg_stat_clear_snapshot();")
        .await
        .unwrap();
    c.query_one(
        "SELECT seq_scan FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'q_work'",
        &[&schema],
    )
    .await
    .unwrap()
    .get(0)
}

/// A claim reads only a handful of claim-index entries, whether the queue is
/// mostly sleeping or mostly due (review finding: the scan must stop after
/// `limit` claimable items).
#[tokio::test]
async fn claims_read_few_index_entries() {
    let Some((mut c, sp)) = setup("sleepy").await else {
        return;
    };
    // 5,000 sleeping items, then 3 due ones.
    for b in 0..5 {
        let items: Vec<String> = (0..1000)
            .map(|i| format!("{{key: s{b}-{i}, priority: {}, after: 1h}}", i % 9))
            .collect();
        run(
            &mut c,
            &sp,
            &format!(
                "request_id: f{b}\nstatements:\n  - insert: {{into: work, rows: [{}]}}\n",
                items.join(", ")
            ),
        )
        .await;
    }
    run(&mut c, &sp, "request_id: due\nstatements:\n  - insert: {into: work, rows: [{key: d1}, {key: d2}, {key: d3}]}\n").await;
    let schema: String = c
        .query_one(
            "SELECT 'fws_' || id FROM fireweed.spaces WHERE tenant = 't' AND space = $1",
            &[&sp],
        )
        .await
        .unwrap()
        .get(0);
    c.batch_execute(&format!("ANALYZE \"{schema}\".q_work"))
        .await
        .unwrap();
    let before = index_reads(&c, &schema).await;
    let r = run(
        &mut c,
        &sp,
        "request_id: c1\nstatements:\n  - claim: {from: work, limit: 10, lease: 30s, worker: w}\n",
    )
    .await;
    assert_eq!(rows(&r, 0).len(), 3);
    let read = index_reads(&c, &schema).await - before;
    assert!(
        read < 50,
        "a claim read {read} index entries of a mostly sleeping queue"
    );
}

/// Index entries read from the queue table's indexes so far.
async fn index_reads(c: &Client, schema: &str) -> i64 {
    c.batch_execute("SELECT pg_stat_force_next_flush(); SELECT pg_stat_clear_snapshot();")
        .await
        .unwrap();
    c.query_one(
        "SELECT coalesce(sum(idx_tup_read), 0)::bigint FROM pg_stat_user_indexes WHERE schemaname = $1 AND relname = 'q_work'",
        &[&schema],
    )
    .await
    .unwrap()
    .get(0)
}

/// An update reschedules and reprioritises unleased items in bulk.
#[tokio::test]
async fn update_reschedules_unleased_items() {
    let Some((mut c, sp)) = setup("resched").await else {
        return;
    };
    run(&mut c, &sp, "request_id: f\nstatements:\n  - insert: {into: work, rows: [{key: a, campaign: x}, {key: b, campaign: x}]}\n").await;
    let r = run(&mut c, &sp, r#"
request_id: u1
statements:
  - update: {target: work, where: {eq: [campaign, x]}, set: {due_at: "2100-01-01T00:00:00Z", priority: 7}}
  - claim: {from: work, limit: 5, lease: 30s, worker: w}
  - select: {from: work, where: {eq: [campaign, x]}, fields: [key, priority, state], order_by: [{field: key}], limit: 5}
"#).await;
    assert_eq!(r.results[0]["matched"], json!(2));
    assert!(rows(&r, 1).is_empty(), "rescheduled items are not due");
    assert_eq!(
        rows(&r, 2),
        vec![
            json!({"key": "a", "priority": 7, "state": "pending"}),
            json!({"key": "b", "priority": 7, "state": "pending"}),
        ]
    );
    // Cancelling: state complete.
    let r = run(
        &mut c,
        &sp,
        r#"
request_id: u2
statements:
  - update: {target: work, keys: [a], set: {state: complete}}
  - select: {from: work, where: {eq: [key, a]}, fields: [state], limit: 1}
"#,
    )
    .await;
    assert_eq!(rows(&r, 1), vec![json!({"state": "complete"})]);
}

/// An update never leaves a matched item holding a lapsed lease: the old
/// token can't settle afterwards (ADR-027 D6).
#[tokio::test]
async fn update_retires_lapsed_leases() {
    let Some((mut c, sp)) = setup("lapsed").await else {
        return;
    };
    set_now(&c, "2030-01-01T00:00:00Z").await;
    run(
        &mut c,
        &sp,
        "request_id: f\nstatements:\n  - insert: {into: work, rows: [{key: a, campaign: x}]}\n",
    )
    .await;
    let r = run(
        &mut c,
        &sp,
        "request_id: c1\nstatements:\n  - claim: {from: work, limit: 1, lease: 10s, worker: w}\n",
    )
    .await;
    let it = rows(&r, 0)[0].clone();
    set_now(&c, "2030-01-01T00:01:00Z").await;
    let r = run(
        &mut c,
        &sp,
        r#"
request_id: u1
statements:
  - update: {target: work, where: {eq: [campaign, x]}, set: {due_at: "2030-01-01T01:00:00Z"}}
  - select: {from: work, where: {eq: [key, a]}, fields: [state, due_at], limit: 1}
"#,
    )
    .await;
    assert_eq!(r.results[0]["matched"], json!(1));
    assert_eq!(rows(&r, 1)[0]["state"], "pending");
    let r = run(&mut c, &sp, &format!(
        "request_id: s1\nstatements:\n  - settle: {{queue: work, outcome: complete, items: [{{id: {}, token: \"{}\"}}]}}\n",
        it["id"], it["token"].as_str().unwrap()
    )).await;
    assert_eq!(rows(&r, 0)[0]["status"], "stale");
}

/// An insert that waits on a concurrent insert of the same key reports the
/// winner's id, not null.
#[tokio::test]
async fn insert_racing_the_same_key_reports_its_id() {
    let Some((mut c, sp)) = setup("race-key").await else {
        return;
    };
    let schema: String = c
        .query_one(
            "SELECT 'fws_' || id FROM fireweed.spaces WHERE tenant = 't' AND space = $1",
            &[&sp],
        )
        .await
        .unwrap()
        .get(0);
    let mut other = connect().await.unwrap();
    let tx = other.transaction().await.unwrap();
    tx.batch_execute(&format!(
        "INSERT INTO \"{schema}\".q_work (key, state, priority, due_at, attempts, version, created_at, updated_at)
         VALUES ('k1', 'pending', 0, now(), 0, 1, now(), now())"
    ))
    .await
    .unwrap();
    let sp2 = sp.clone();
    let waiting = tokio::spawn(async move {
        let mut c2 = connect().await.unwrap();
        run(
            &mut c2,
            &sp2,
            "request_id: i1\nstatements:\n  - insert: {into: work, rows: [{key: k1}]}\n",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    tx.commit().await.unwrap();
    let r = waiting.await.unwrap();
    let e = &rows(&r, 0)[0];
    assert_eq!(e["status"], "exists");
    assert!(e["id"].is_i64(), "{e}");
    // The record replays the same id.
    let again = run(
        &mut c,
        &sp,
        "request_id: i1\nstatements:\n  - insert: {into: work, rows: [{key: k1}]}\n",
    )
    .await;
    assert!(again.replayed);
    assert_eq!(rows(&again, 0)[0]["id"], e["id"]);
}

/// A replayed claim returns the items with the tokens it granted, re-read:
/// the record keeps only ids and tokens.
#[tokio::test]
async fn replayed_claims_reread_their_items() {
    let Some((mut c, sp)) = setup("replay-claim").await else {
        return;
    };
    run(&mut c, &sp, "request_id: f\nstatements:\n  - insert: {into: work, rows: [{key: a, campaign: x, body: {n: 1}}]}\n").await;
    let doc =
        "request_id: c1\nstatements:\n  - claim: {from: work, limit: 1, lease: 30s, worker: w}\n";
    let first = run(&mut c, &sp, doc).await;
    let again = run(&mut c, &sp, doc).await;
    assert!(again.replayed);
    assert_eq!(again.results, first.results);
    let stored: String = c
        .query_one(
            &format!(
                "SELECT response::text FROM {}.fw_requests WHERE request_id = 'c1'",
                c.query_one(
                    "SELECT 'fws_' || id FROM fireweed.spaces WHERE tenant = 't' AND space = $1",
                    &[&sp]
                )
                .await
                .unwrap()
                .get::<_, String>(0)
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!stored.contains("\"body\""), "{stored}");
}

/// `on_unmet: report`: an unmet expectation changes nothing for that
/// statement and reports `met: false`; the rest of the program commits.
#[tokio::test]
async fn unmet_expectations_can_report_instead_of_rejecting() {
    let Some((mut c, sp)) = setup("report").await else {
        return;
    };
    let r = run(&mut c, &sp, r#"
request_id: g1
statements:
  - insert: {into: slots, rows: [{contact: a, hour: "2030-01-01T10:00:00Z", count: 2}, {contact: b, hour: "2030-01-01T10:00:00Z", count: 0}]}
  - update:
      target: slots
      keys: [{contact: a, hour: "2030-01-01T10:00:00Z"}]
      where: {lt: [count, 2]}
      set: {count: {add: [{field: count}, 1]}}
      expect: {min: 1, max: 1, on_unmet: report}
  - update:
      target: slots
      keys: [{contact: b, hour: "2030-01-01T10:00:00Z"}]
      where: {lt: [count, 2]}
      set: {count: {add: [{field: count}, 1]}}
      expect: {min: 1, max: 1, on_unmet: report}
  - assert: {target: supp, keys: [{address: nobody}], expect: {min: 1, on_unmet: report}}
  - select: {from: slots, where: {in: [contact, [a, b]]}, fields: [contact, count], order_by: [{field: contact}], limit: 2}
"#).await;
    assert_eq!(r.status, Status::Committed);
    assert_eq!(r.results[1], json!({"matched": 0, "met": false}));
    assert_eq!(r.results[2], json!({"matched": 1, "met": true}));
    assert_eq!(r.results[3], json!({"matched": 0, "met": false}));
    assert_eq!(
        rows(&r, 4),
        vec![
            json!({"contact": "a", "count": 2}),
            json!({"contact": "b", "count": 1})
        ]
    );
}

/// Guards read other objects of the space through indexed lookups:
/// a claim skips suppressed contacts and contacts whose resolved entity
/// already has a completed item, and an assert checks one address.
#[tokio::test]
async fn guards_look_up_other_objects_in_the_space() {
    let Some((mut c, sp)) = setup("lookups").await else {
        return;
    };
    run(
        &mut c,
        &sp,
        r#"
request_id: f
statements:
  - insert: {into: supp, rows: [{address: b@x, reasons: [bounce]}]}
  - insert:
      into: work
      rows:
        - {key: done, email: d@x, entity: e1}
        - {key: a, email: a@x, entity: e2}
        - {key: b, email: b@x, entity: e3}
        - {key: d, email: d2@x, entity: e1}
"#,
    )
    .await;
    // Complete the first e1 item, so e1 is already served.
    let r = run(
        &mut c,
        &sp,
        "request_id: c0\nstatements:\n  - claim: {from: work, limit: 1, lease: 30s, worker: w}\n",
    )
    .await;
    let first = rows(&r, 0)[0].clone();
    assert_eq!(first["key"], "done");
    run(&mut c, &sp, &format!(
        "request_id: s0\nstatements:\n  - settle: {{queue: work, outcome: complete, items: [{{id: {}, token: \"{}\"}}]}}\n",
        first["id"], first["token"].as_str().unwrap()
    )).await;
    let r = run(&mut c, &sp, r#"
request_id: c1
statements:
  - claim:
      from: work
      limit: 10
      lease: 30s
      worker: w
      where:
        and:
          - {not: {exists: {in: supp, match: {address: {field: email}}}}}
          - {not: {exists: {in: work, match: {entity: {field: entity}}, where: {eq: [state, complete]}}}}
  - assert: {target: supp, keys: [{address: b@x}], expect: {max: 0, on_unmet: report}}
"#).await;
    // b is suppressed; d's entity e1 already has a completed item.
    assert_eq!(
        rows(&r, 0)
            .iter()
            .map(|i| i["key"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["a"]
    );
    assert_eq!(r.results[1], json!({"matched": 1, "met": false}));
}

/// Counts need no equality term when one index holds every column they read,
/// and they count every row: no cap. Expired leases count as pending.
#[tokio::test]
async fn counts_read_an_index() {
    let Some((mut c, sp)) = setup("counts").await else {
        return;
    };
    for b in 0..5 {
        let items: Vec<String> = (0..1000)
            .map(|i| format!("{{key: k{b}-{i}, campaign: c{}}}", i % 4))
            .collect();
        run(
            &mut c,
            &sp,
            &format!(
                "request_id: f{b}\nstatements:\n  - insert: {{into: work, rows: [{}]}}\n",
                items.join(", ")
            ),
        )
        .await;
    }
    run(
        &mut c,
        &sp,
        "request_id: l1\nstatements:\n  - claim: {from: work, limit: 300, lease: 30s, worker: w}\n",
    )
    .await;
    let table: String = c
        .query_one(
            "SELECT 'fws_' || id FROM fireweed.spaces WHERE tenant = 't' AND space = $1",
            &[&sp],
        )
        .await
        .unwrap()
        .get(0);
    c.batch_execute(&format!("VACUUM ANALYZE \"{table}\".q_work"))
        .await
        .unwrap();
    let count = r#"
statements:
  - select: {from: work, aggregate: {n: count}, group_by: [state], order_by: [{field: state}], limit: 5}
  - select: {from: work, aggregate: {n: count}, group_by: [campaign], order_by: [{field: campaign}], limit: 10}
  - select: {from: work, where: {gt: [campaign, c1]}, aggregate: {n: count}, limit: 1}
"#;
    let before = seq_scans(&c, &table).await;
    let r = run(&mut c, &sp, &format!("request_id: n1{count}")).await;
    assert_eq!(
        Value::Array(rows(&r, 0)),
        json!([{"state": "leased", "n": 300}, {"state": "pending", "n": 4700}])
    );
    assert_eq!(rows(&r, 1).len(), 4);
    assert_eq!(rows(&r, 1)[0], json!({"campaign": "c0", "n": 1250}));
    assert_eq!(rows(&r, 2), vec![json!({"n": 2500})]);
    // Once the leases lapse they count as pending, with nothing written.
    set_now(&c, "2100-01-01T00:00:00Z").await;
    let r = run(&mut c, &sp, &format!("request_id: n2{count}")).await;
    assert_eq!(rows(&r, 0), vec![json!({"state": "pending", "n": 5000})]);
    c.batch_execute("RESET fireweed.now").await.unwrap();
    assert_eq!(
        seq_scans(&c, &table).await,
        before,
        "a count scanned the whole queue table"
    );
    // A column outside the index needs an equality term.
    let e = try_run(
        &mut c,
        &sp,
        "request_id: n3\nstatements:\n  - select: {from: work, aggregate: {n: count}, group_by: [step], limit: 5}\n",
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::Invalid(_)), "{e}");
}

/// A request issued before the horizon is refused, not run again, even
/// after its record is swept and the clock goes back; one issued far ahead
/// of the clock is invalid (ADR-027 D61).
#[tokio::test]
async fn expired_requests_are_refused_not_rerun() {
    let Some((mut c, sp)) = setup("expiry").await else {
        return;
    };
    set_now(&c, "2030-01-01T00:00:00Z").await;
    let insert = |rid: &str, at: &str| {
        format!(
            "request_id: {rid}\nissued_at: \"{at}\"\nstatements:\n  - insert: {{into: work, rows: [{{key: {rid}}}]}}\n"
        )
    };
    let e = try_run(&mut c, &sp, &insert("old", "2029-12-30T00:00:00Z"))
        .await
        .unwrap_err();
    assert_eq!(e, Error::RequestExpired);
    let e = try_run(&mut c, &sp, &insert("ahead", "2030-01-01T00:10:00Z"))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Invalid(_)), "{e}");

    let doc = insert("once", "2030-01-01T00:00:00Z");
    assert!(!run(&mut c, &sp, &doc).await.replayed);
    assert!(run(&mut c, &sp, &doc).await.replayed);
    // Past retention the record may be gone, so the id is refused.
    set_now(&c, "2030-01-03T00:00:00Z").await;
    assert_eq!(
        try_run(&mut c, &sp, &doc).await.unwrap_err(),
        Error::RequestExpired
    );
    let swept = sweep(&mut c, "t", &sp, 100).await.unwrap();
    assert!(swept.deleted >= 1, "{swept:?}");
    // The horizon is stored, so a clock that goes back can't let it run again.
    set_now(&c, "2030-01-01T00:00:01Z").await;
    assert_eq!(
        try_run(&mut c, &sp, &doc).await.unwrap_err(),
        Error::RequestExpired
    );
    let r = run(
        &mut c,
        &sp,
        "statements:\n  - select: {from: work, where: {eq: [key, once]}, aggregate: {n: count}, limit: 1}\n",
    )
    .await;
    assert_eq!(rows(&r, 0), vec![json!({"n": 1})]);
}

/// Versions never repeat for a key, even after a delete and a reinsert, so a
/// stale compare-and-set can't match the new row (ADR-027 D61).
#[tokio::test]
async fn versions_never_repeat_after_reinsert() {
    let Some((mut c, sp)) = setup("versions").await else {
        return;
    };
    let version = |r: &Response| rows(r, 1)[0]["version"].as_i64().unwrap();
    let get = "  - select: {from: work, where: {eq: [key, v]}, fields: [version], limit: 1}\n";
    let r = run(
        &mut c,
        &sp,
        &format!(
            "request_id: v1\nstatements:\n  - insert: {{into: work, rows: [{{key: v}}]}}\n{get}"
        ),
    )
    .await;
    let first = version(&r);
    run(
        &mut c,
        &sp,
        "request_id: v2\nstatements:\n  - delete: {target: work, keys: [v]}\n",
    )
    .await;
    let r = run(
        &mut c,
        &sp,
        &format!(
            "request_id: v3\nstatements:\n  - insert: {{into: work, rows: [{{key: v}}]}}\n{get}"
        ),
    )
    .await;
    assert!(version(&r) > first, "{} after {first}", version(&r));
    // A compare-and-set holding the old version matches nothing.
    let r = run(
        &mut c,
        &sp,
        &format!(
            "request_id: v4\nstatements:\n  - update: {{target: work, keys: [v], where: {{eq: [version, {first}]}}, set: {{step: 1}}, expect: {{min: 1, max: 1}}}}\n"
        ),
    )
    .await;
    assert_eq!(r.rejected.unwrap().reason, RejectReason::ExpectUnmet);
}

/// A batch commits once; each program succeeds, rejects or fails on its own
/// (ADR-027 D62).
#[tokio::test]
async fn batch_programs_are_independent() {
    let Some((mut c, sp)) = setup("batch").await else {
        return;
    };
    let docs = [
        "request_id: b1\nstatements:\n  - insert: {into: work, rows: [{key: k1}]}\n",
        "request_id: b2\nstatements:\n  - insert: {into: work, rows: [{key: k1}], on_conflict: reject}\n",
        "request_id: b3\nstatements:\n  - insert: {into: nowhere, rows: [{key: k3}]}\n",
        "request_id: b4\nstatements:\n  - insert: {into: work, rows: [{key: k2}]}\n",
    ];
    let mut programs = vec![];
    for d in docs {
        programs.push(program(&c, &sp, d).await);
    }
    let out = execute_batch(&mut c, "t", &sp, &programs).await.unwrap();
    assert_eq!(out[0].as_ref().unwrap().status, Status::Committed);
    assert_eq!(
        out[1].as_ref().unwrap().rejected.as_ref().unwrap().reason,
        RejectReason::KeyConflict
    );
    assert!(matches!(out[2], Err(Error::Invalid(_))), "{:?}", out[2]);
    assert_eq!(out[3].as_ref().unwrap().status, Status::Committed);
    let r = run(
        &mut c,
        &sp,
        "statements:\n  - select: {from: work, where: {in: [key, [k1, k2, k3]]}, aggregate: {n: count}, limit: 1}\n",
    )
    .await;
    assert_eq!(rows(&r, 0), vec![json!({"n": 2})]);
    // Sent again, every recorded program replays; the rejection too.
    let again = execute_batch(&mut c, "t", &sp, &programs).await.unwrap();
    assert!(again[0].as_ref().unwrap().replayed);
    assert!(again[1].as_ref().unwrap().replayed);
    assert!(again[3].as_ref().unwrap().replayed);
}
