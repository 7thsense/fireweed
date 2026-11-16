//! Snorri workflow-DSL patterns written as Fireweed requests, driven
//! through their edge cases. The mapping follows the local-engine plan's
//! DSL check (Part 2): an instance is a queue item with a JSON body; a wait
//! is a re-arm to a later time; an event is a row; a wake is three
//! operations on one instance in one group; guards are `Assert`s.
//!
//! Every scenario checks final state through `get` or `read`, not only
//! statuses.

use std::collections::BTreeSet;
use std::sync::Arc;

use fireweed_interface::*;
use futures::executor::block_on;
use serde_json::json;

use crate::ensure;
use crate::support::*;

type Test = fn(&Fixture) -> T;

pub const ALL: &[(&str, Test)] = &[
    ("DSL-1", dsl_timer_instant),
    ("DSL-2", dsl_timer_attempts),
    ("DSL-3", dsl_wait_then_event),
    ("DSL-4", dsl_event_then_wait),
    ("DSL-5", dsl_wait_event_race),
    ("DSL-6", dsl_wake_two_awaits),
    ("DSL-7", dsl_duplicate_out_of_order_events),
    ("DSL-8", dsl_fan_in),
    ("DSL-9", dsl_send_suppressed),
    ("DSL-10", dsl_crash_loop),
    ("DSL-11", dsl_cancel_run),
    ("DSL-12", dsl_schedule_tick),
    ("DSL-13", dsl_timeline),
    ("DSL-14", dsl_rate_cap),
];

const FAR: i64 = 86_400_000;

// ------------------------------------------------------------------ setup

/// The base schema plus the DSL objects: `inst` (workflow instances),
/// `effects` (send intents) and `events` (external events by run and
/// correlation key).
fn setup(fx: &Fixture) -> T {
    use ScalarType::*;
    let mut s = base_schema();
    let mut spec = QueueSpec::basic(PriorityKind::Int64);
    spec.recurrence = true;
    spec.max_attempts = 3;
    s.queues.push(QueueDecl {
        name: o("inst"),
        spec,
        body: BodyKind::Json,
        fields: vec![
            FieldDecl::stored("run", Text, true, IndexHint::Lookup),
            FieldDecl::stored("node", Text, true, IndexHint::Lookup),
            FieldDecl::stored("awaits", TextSet, true, IndexHint::Lookup),
            FieldDecl::stored("pending", Int, true, IndexHint::None),
        ],
    });
    s.queues.push(QueueDecl {
        name: o("effects"),
        spec: QueueSpec::basic(PriorityKind::Int64),
        body: BodyKind::Json,
        fields: vec![FieldDecl::stored("run", Text, true, IndexHint::Lookup)],
    });
    s.tables.push(TableDecl {
        name: o("events"),
        key: vec![f("run"), f("corr")],
        fields: vec![
            FieldDecl::stored("run", Text, false, IndexHint::None),
            FieldDecl::stored("corr", Text, false, IndexHint::None),
            FieldDecl::stored("payload", Json, true, IndexHint::None),
        ],
        expiry: None,
        key_assignment: KeyAssignment::Caller,
    });
    let r = ok(fx.upgrade(s), "DSL schema")?;
    ensure!(r.changed, "DSL schema not applied");
    Ok(())
}

fn hdr(id: &str) -> WriteHeader {
    WriteHeader::new(sp(), id)
}

fn commit_on(
    p: &Arc<dyn DataPlane>,
    id: &str,
    ops: Vec<WriteOp>,
    ps: Params,
) -> DpResult<CommitResponse> {
    block_on(p.commit(CommitRequest {
        header: hdr(id),
        ops,
        params: ps,
        dry_run: false,
    }))
}

fn new_instance(key: &str, run: &str, node: &str, prio: i64) -> NewItem {
    let mut it = NewItem::new(Some(key), PriorityValue::Int(prio));
    it.body = Some(Body::Json(json!({ "inbox": [], "wakes": [] })));
    it.fields.insert(f("run"), text(run));
    it.fields.insert(f("node"), text(node));
    it
}

fn start(fx: &Fixture, key: &str, run: &str, node: &str, prio: i64) -> Result<ItemId, Fail> {
    let r = ok(
        fx.commit(vec![WriteOp::Enqueue {
            queue: o("inst"),
            items: vec![new_instance(key, run, node, prio)],
            on_key_conflict: KeyConflict::Reject,
        }]),
        "start",
    )?;
    ensure!(r.status == CommitStatus::Committed, "start {key}: {r:?}");
    r.ops[0].entries[0]
        .item_id
        .ok_or_else(|| Fail::Failed("no item id".into()))
}

fn claim_on(
    fx: &Fixture,
    p: &Arc<dyn DataPlane>,
    filter: Option<Expr>,
    lease_ms: u64,
) -> Result<Option<ClaimedItem>, Fail> {
    let mut req = ClaimRequest::new(hdr(&fx.rid()), "inst", "w", 1, lease_ms);
    req.filter = filter;
    let r = ok(block_on(p.claim(req)), "claim")?;
    Ok(r.items.into_iter().next())
}

fn claim(fx: &Fixture) -> Result<Option<ClaimedItem>, Fail> {
    claim_on(fx, &fx.plane, None, 60_000)
}

fn claim_run(fx: &Fixture, run: &str) -> Result<Option<ClaimedItem>, Fail> {
    claim_on(
        fx,
        &fx.plane,
        Some(Expr::eq(Expr::field("run"), Expr::text(run))),
        60_000,
    )
}

fn must(c: Option<ClaimedItem>, what: &str) -> Result<ClaimedItem, Fail> {
    c.ok_or_else(|| Fail::Failed(format!("{what}: nothing claimed")))
}

fn view(fx: &Fixture, id: ItemId) -> Result<ItemView, Fail> {
    fx.get_in("inst", id)
        .ok_or_else(|| Fail::Failed(format!("instance {id:?} missing")))
}

fn body(v: &ItemView) -> serde_json::Value {
    match &v.body {
        Some(Body::Json(j)) => j.clone(),
        _ => serde_json::Value::Null,
    }
}

fn awaits(v: &ItemView) -> BTreeSet<String> {
    match v.fields.get(&f("awaits")) {
        Some(Value::TextSet(s)) => s.clone(),
        _ => BTreeSet::new(),
    }
}

fn finalize_op(c: &ClaimedItem, outcome: FinalizeOutcome) -> FinalizeEntry {
    FinalizeEntry::new(lease_of(c), outcome)
}

fn fin(e: FinalizeEntry) -> WriteOp {
    WriteOp::Finalize {
        queue: o("inst"),
        entries: vec![e],
    }
}

fn rearm(at: Schedule) -> FinalizeOutcome {
    FinalizeOutcome::Rearm {
        not_before: at,
        priority: None,
    }
}

fn now() -> Schedule {
    Schedule::After { delay_ms: 0 }
}

fn waiter_pred() -> Expr {
    Expr::And(vec![
        Expr::eq(Expr::field("run"), Expr::param("run")),
        Expr::Contains {
            set: Box::new(Expr::field("awaits")),
            value: Box::new(Expr::param("corr")),
        },
    ])
}

fn ev_key(run: &str, corr: &str) -> Vec<Value> {
    vec![text(run), text(corr)]
}

fn ev_params(run: &str, corr: &str, payload: serde_json::Value) -> Params {
    params(vec![
        ("run", text(run)),
        ("corr", text(corr)),
        ("payload", Value::Json(payload)),
    ])
}

/// The worker's wait: register the await and park the instance, unless the
/// event is already stored. Re-arming does not charge an attempt.
fn wait_ops(c: &ClaimedItem, run: &str, corr: &str) -> Vec<WriteOp> {
    let mut e = finalize_op(c, rearm(Schedule::At(UtcMillis(START + FAR))));
    e.set = vec![(
        f("awaits"),
        Expr::SetAdd {
            set: Box::new(Expr::field("awaits")),
            values: vec![Expr::text(corr)],
        },
    )];
    vec![
        assert_op("events", row_sel(ev_key(run, corr), None), 0, 0),
        fin(e),
    ]
}

/// The worker found the event already stored: consume it and continue now.
fn consume_ops(c: &ClaimedItem, run: &str, corr: &str, ev: &RowView) -> Vec<WriteOp> {
    let payload = ev
        .fields
        .get(&f("payload"))
        .cloned()
        .unwrap_or(Value::Null(ScalarType::Json));
    let mut e = finalize_op(c, rearm(now()));
    e.patch = vec![BodyPatch::Append {
        pointer: JsonPointer::new("/inbox"),
        value: Expr::Lit(payload),
        keep_last: None,
    }];
    vec![
        WriteOp::Mutate(Mutate::new(
            "events",
            Selection::Keys {
                keys: vec![Keyed {
                    key: KeyRef::Row(ev_key(run, corr)),
                    expected_version: Some(ev.row_version),
                }],
                filter: None,
            },
            Action::Retire,
            1,
        )),
        fin(e),
    ]
}

/// The event's first group: store it, unless an instance already waits.
fn store_event_ops(run: &str, corr: &str, payload: serde_json::Value) -> Vec<WriteOp> {
    vec![
        assert_op(
            "inst",
            Selection::Where {
                predicate: waiter_pred(),
            },
            0,
            0,
        ),
        put_rows(
            "events",
            vec![row_of(vec![
                ("run", text(run)),
                ("corr", text(corr)),
                ("payload", Value::Json(payload)),
            ])],
            RowConflict::Reject,
        ),
    ]
}

/// The wake: three operations on the waiting instance in one group (there
/// is no `Action::Seq`): append to the inbox, make it due now, remove the
/// await. Each must match exactly one instance.
fn wake_ops() -> Vec<WriteOp> {
    let one = |action: Action| {
        let mut m = Mutate::new(
            "inst",
            Selection::Where {
                predicate: waiter_pred(),
            },
            action,
            1,
        );
        m.expect = Some(MatchExpect { min: 1, max: 1 });
        WriteOp::Mutate(m)
    };
    vec![
        one(Action::Patch(vec![BodyPatch::Append {
            pointer: JsonPointer::new("/inbox"),
            value: Expr::param("payload"),
            keep_last: None,
        }])),
        one(Action::Reschedule {
            not_before: ScheduleChange::Set(Expr::Now),
            priority: None,
        }),
        one(Action::Set(vec![(
            f("awaits"),
            Expr::SetRemove {
                set: Box::new(Expr::field("awaits")),
                values: vec![Expr::param("corr")],
            },
        )])),
    ]
}

#[derive(Debug, PartialEq)]
enum Delivered {
    Stored,
    Woke,
}

/// Delivers an event: the request id is the event id, so a duplicate
/// delivery replays whichever path the original took.
fn deliver(
    p: &Arc<dyn DataPlane>,
    event_id: &str,
    run: &str,
    corr: &str,
    payload: serde_json::Value,
) -> Result<Delivered, Fail> {
    let ps = ev_params(run, corr, payload.clone());
    let ops = store_event_ops(run, corr, payload);
    let r = ok(commit_on(p, event_id, ops, ps.clone()), "store event")?;
    if r.status == CommitStatus::Committed {
        return Ok(Delivered::Stored);
    }
    ensure!(
        failure_of(&r) == Some(&FailureReason::MatchExpectUnmet { matched: 1 }),
        "event {event_id} rejected for another reason: {r:?}"
    );
    let w = ok(
        commit_on(p, &format!("{event_id}:wake"), wake_ops(), ps),
        "wake",
    )?;
    ensure!(
        w.status == CommitStatus::Committed,
        "wake {event_id}: {w:?}"
    );
    Ok(Delivered::Woke)
}

#[derive(Debug, PartialEq)]
enum Awaited {
    Parked,
    Consumed,
}

/// The worker's await on a claimed instance.
fn await_event(
    fx: &Fixture,
    p: &Arc<dyn DataPlane>,
    c: &ClaimedItem,
    run: &str,
    corr: &str,
) -> Result<Awaited, Fail> {
    let r = ok(
        commit_on(p, &fx.rid(), wait_ops(c, run, corr), Params::new()),
        "wait",
    )?;
    if r.status == CommitStatus::Committed {
        return Ok(Awaited::Parked);
    }
    ensure!(
        failure_of(&r) == Some(&FailureReason::MatchExpectUnmet { matched: 1 }),
        "wait rejected for another reason: {r:?}"
    );
    let ev = ok(
        block_on(p.get(GetRequest {
            space: sp(),
            targets: vec![GetTarget::Row {
                table: o("events"),
                key: ev_key(run, corr),
            }],
        })),
        "read event",
    )?;
    let Some(Some(GetResult::Row(ev))) = ev.results.into_iter().next() else {
        return Err(Fail::Failed("event vanished".into()));
    };
    let r = ok(
        commit_on(p, &fx.rid(), consume_ops(c, run, corr, &ev), Params::new()),
        "consume",
    )?;
    ensure!(r.status == CommitStatus::Committed, "consume: {r:?}");
    Ok(Awaited::Consumed)
}

/// After an instance received `inbox`, it is due now, awaits nothing, and
/// no event row is left.
fn check_woken(fx: &Fixture, id: ItemId, run: &str, corrs: &[&str], inbox: serde_json::Value) -> T {
    let v = view(fx, id)?;
    ensure!(
        v.state == ItemState::Pending && v.not_before.is_some_and(|t| t.0 <= fx.clock.now().0),
        "instance not due: {v:?}"
    );
    ensure!(body(&v)["inbox"] == inbox, "inbox: {}", body(&v)["inbox"]);
    ensure!(awaits(&v).is_empty(), "awaits left: {:?}", awaits(&v));
    for c in corrs {
        ensure!(
            fx.get_row_key("events", ev_key(run, c)).is_none(),
            "event {c} not consumed"
        );
    }
    Ok(())
}

// --------------------------------------------------------------- timers

/// A timer is due exactly at its instant: not a millisecond before; two
/// instances due at one instant are claimed in priority order.
fn dsl_timer_instant(fx: &Fixture) -> T {
    setup(fx)?;
    let a = start(fx, "ta", "r", "wait", 1)?;
    let b = start(fx, "tb", "r", "wait", 2)?;
    let due = START + 5000;
    for _ in 0..2 {
        let c = must(claim(fx)?, "first claim")?;
        ok(
            fx.commit(vec![fin(finalize_op(
                &c,
                rearm(Schedule::At(UtcMillis(due))),
            ))]),
            "park",
        )?;
    }
    fx.advance(4999);
    ensure!(claim(fx)?.is_none(), "claimed before the instant");
    fx.advance(1);
    let c = must(claim(fx)?, "at the instant")?;
    ensure!(c.item_id == a, "priority order at the instant");
    fx.advance(1);
    let c2 = must(claim(fx)?, "after the instant")?;
    ensure!(c2.item_id == b, "second instance");
    ensure!(
        view(fx, a)?.not_before == Some(UtcMillis(due)),
        "not_before changed by the claim"
    );
    Ok(())
}

/// Waiting through `Retry` charges attempts and ends in
/// `attempts-exhausted`; waiting through `Rearm` does not charge.
fn dsl_timer_attempts(fx: &Fixture) -> T {
    setup(fx)?;
    let a = start(fx, "retry", "r", "wait", 1)?;
    for i in 1..=3 {
        let c = must(claim_run(fx, "r")?, "claim retry")?;
        ensure!(c.attempt_count == i, "attempt {i}: {}", c.attempt_count);
        ok(
            fx.commit(vec![fin(finalize_op(
                &c,
                FinalizeOutcome::Retry {
                    not_before: Schedule::After { delay_ms: 10 },
                    priority: None,
                },
            ))]),
            "retry",
        )?;
        fx.advance(10);
    }
    let v = view(fx, a)?;
    ensure!(
        v.state == ItemState::Failed
            && v.terminal.as_ref().and_then(|t| t.code.as_deref()) == Some("attempts-exhausted"),
        "Retry waits did not exhaust attempts: {v:?}"
    );
    let b = start(fx, "rearm", "q", "wait", 1)?;
    for _ in 0..6 {
        let c = must(claim_run(fx, "q")?, "claim rearm")?;
        ensure!(c.attempt_count == 1, "Rearm charged: {}", c.attempt_count);
        ok(
            fx.commit(vec![fin(finalize_op(
                &c,
                rearm(Schedule::After { delay_ms: 10 }),
            ))]),
            "rearm",
        )?;
        fx.advance(10);
    }
    ensure!(view(fx, b)?.state == ItemState::Pending, "Rearm wait ended");
    Ok(())
}

// ------------------------------------------------------- waits and events

/// The instance waits first; the event finds the waiter and wakes it.
fn dsl_wait_then_event(fx: &Fixture) -> T {
    setup(fx)?;
    let id = start(fx, "w1", "r1", "wait", 1)?;
    let c = must(claim(fx)?, "claim")?;
    ensure!(
        await_event(fx, &fx.plane, &c, "r1", "c1")? == Awaited::Parked,
        "nothing stored yet, so the wait parks"
    );
    ensure!(
        awaits(&view(fx, id)?) == set_of(&["c1"]),
        "await registered"
    );
    ensure!(claim(fx)?.is_none(), "a parked instance was claimable");
    let d = deliver(&fx.plane, "e1", "r1", "c1", json!({ "n": 1 }))?;
    ensure!(d == Delivered::Woke, "event did not wake the waiter: {d:?}");
    check_woken(fx, id, "r1", &["c1"], json!([{ "n": 1 }]))?;
    let c = must(claim(fx)?, "claim after wake")?;
    ensure!(c.item_id == id, "the woken instance is claimable");
    Ok(())
}

/// The event arrives first and is stored; the instance's wait finds it and
/// consumes it instead of parking.
fn dsl_event_then_wait(fx: &Fixture) -> T {
    setup(fx)?;
    let id = start(fx, "w2", "r2", "wait", 1)?;
    let d = deliver(&fx.plane, "e2", "r2", "c2", json!({ "n": 2 }))?;
    ensure!(d == Delivered::Stored, "no waiter yet: {d:?}");
    ensure!(
        fx.get_row_key("events", ev_key("r2", "c2")).is_some(),
        "event not stored"
    );
    let c = must(claim(fx)?, "claim")?;
    ensure!(
        await_event(fx, &fx.plane, &c, "r2", "c2")? == Awaited::Consumed,
        "the wait did not find the stored event"
    );
    check_woken(fx, id, "r2", &["c2"], json!([{ "n": 2 }]))
}

/// Event and wait race on threads (and instances, with peers). Exactly one
/// side delivers the payload, once.
fn dsl_wait_event_race(fx: &Fixture) -> T {
    setup(fx)?;
    let other = fx.open_peer().unwrap_or_else(|| fx.plane.clone());
    for round in 0..8 {
        let run = format!("race{round}");
        let id = start(fx, &format!("i{round}"), &run, "wait", round)?;
        let c = must(claim_run(fx, &run)?, "claim")?;
        let (a, d) = std::thread::scope(|scope| {
            let w = scope.spawn(|| await_event(fx, &fx.plane, &c, &run, "c"));
            let e = scope.spawn(|| deliver(&other, &format!("ev{round}"), &run, "c", json!(round)));
            (w.join().unwrap(), e.join().unwrap())
        });
        let (a, d) = (a?, d?);
        ensure!(
            matches!(
                (&a, &d),
                (Awaited::Parked, Delivered::Woke) | (Awaited::Consumed, Delivered::Stored)
            ),
            "round {round}: wait {a:?}, event {d:?}"
        );
        check_woken(fx, id, &run, &["c"], json!([round]))?;
    }
    Ok(())
}

/// An instance awaiting two keys: the first event removes only its own
/// await; the second wakes it again. Payloads arrive in delivery order.
fn dsl_wake_two_awaits(fx: &Fixture) -> T {
    setup(fx)?;
    let id = start(fx, "two", "r", "wait", 1)?;
    let c = must(claim(fx)?, "claim")?;
    let mut e = finalize_op(&c, rearm(Schedule::At(UtcMillis(START + FAR))));
    e.set = vec![(f("awaits"), lit_set(&["a", "b"]))];
    ok(fx.commit(vec![fin(e)]), "wait on two")?;
    ensure!(
        deliver(&fx.plane, "ea", "r", "a", json!("A"))? == Delivered::Woke,
        "first event"
    );
    let v = view(fx, id)?;
    ensure!(awaits(&v) == set_of(&["b"]), "after a: {:?}", awaits(&v));
    ensure!(
        deliver(&fx.plane, "eb", "r", "b", json!("B"))? == Delivered::Woke,
        "second event"
    );
    check_woken(fx, id, "r", &["a", "b"], json!(["A", "B"]))?;
    // A third event for a key nobody awaits is stored, not lost.
    ensure!(
        deliver(&fx.plane, "ec", "r", "c", json!("C"))? == Delivered::Stored,
        "unawaited event"
    );
    Ok(())
}

fn lit_set(xs: &[&str]) -> Expr {
    Expr::Lit(Value::TextSet(set_of(xs)))
}

/// Duplicate and out-of-order delivery: replays by event id never add a
/// second payload or recreate a consumed event.
fn dsl_duplicate_out_of_order_events(fx: &Fixture) -> T {
    setup(fx)?;
    let id = start(fx, "dup", "r", "wait", 1)?;
    let c = must(claim(fx)?, "claim")?;
    ensure!(
        await_event(fx, &fx.plane, &c, "r", "c1")? == Awaited::Parked,
        "park on c1"
    );
    // e2 (for c2) arrives before e1.
    ensure!(
        deliver(&fx.plane, "e2", "r", "c2", json!(2))? == Delivered::Stored,
        "e2 stored"
    );
    ensure!(
        deliver(&fx.plane, "e1", "r", "c1", json!(1))? == Delivered::Woke,
        "e1 wakes"
    );
    ensure!(
        deliver(&fx.plane, "e1", "r", "c1", json!(1))? == Delivered::Woke,
        "duplicate e1 replays the wake"
    );
    ensure!(
        body(&view(fx, id)?)["inbox"] == json!([1]),
        "duplicate e1 added a payload"
    );
    let c = must(claim(fx)?, "claim after e1")?;
    ensure!(
        await_event(fx, &fx.plane, &c, "r", "c2")? == Awaited::Consumed,
        "c2 consumed from the store"
    );
    ensure!(
        deliver(&fx.plane, "e2", "r", "c2", json!(2))? == Delivered::Stored,
        "duplicate e2 replays the store"
    );
    check_woken(fx, id, "r", &["c1", "c2"], json!([1, 2]))
}

// ----------------------------------------------------------------- fan-in

/// A join waits for three children. Each child's completion decrements the
/// parent's counter and, in the same group, wakes the parent only when the
/// counter reaches zero. Children complete on several threads; a duplicate
/// completion is refused and does not decrement again.
fn dsl_fan_in(fx: &Fixture) -> T {
    setup(fx)?;
    let mut parent = new_instance("join", "f", "join", 100);
    parent.fields.insert(f("pending"), Value::Int(3));
    parent.not_before = Some(Schedule::At(UtcMillis(START + FAR)));
    let r = ok(
        fx.commit(vec![WriteOp::Enqueue {
            queue: o("inst"),
            items: vec![parent],
            on_key_conflict: KeyConflict::Reject,
        }]),
        "parent",
    )?;
    let pid = r.ops[0].entries[0]
        .item_id
        .ok_or_else(|| Fail::Failed("no parent id".into()))?;
    for i in 0..3 {
        start(fx, &format!("child{i}"), "f", "child", i)?;
    }
    let child_filter = Expr::eq(Expr::field("node"), Expr::text("child"));
    let mut claimed = vec![];
    for _ in 0..3 {
        claimed.push(must(
            claim_on(fx, &fx.plane, Some(child_filter.clone()), 60_000)?,
            "claim child",
        )?);
    }
    let complete_ops = |c: &ClaimedItem| {
        let dec = Mutate::new(
            "inst",
            item_sel(pid, None),
            Action::Set(vec![(
                f("pending"),
                Expr::arith(ArithOp::Sub, Expr::field("pending"), Expr::int(1)),
            )]),
            1,
        );
        let mut wake = Mutate::new(
            "inst",
            item_sel(pid, Some(Expr::eq(Expr::field("pending"), Expr::int(0)))),
            Action::Patch(vec![BodyPatch::Append {
                pointer: JsonPointer::new("/wakes"),
                value: Expr::text("go"),
                keep_last: None,
            }]),
            1,
        );
        wake.expect = Some(MatchExpect { min: 0, max: 1 });
        let mut due = wake.clone();
        due.action = Action::Reschedule {
            not_before: ScheduleChange::Set(Expr::Now),
            priority: None,
        };
        vec![
            fin(finalize_op(c, FinalizeOutcome::Complete)),
            WriteOp::Mutate(dec),
            WriteOp::Mutate(wake),
            WriteOp::Mutate(due),
        ]
    };
    let other = fx.open_peer().unwrap_or_else(|| fx.plane.clone());
    let results: Vec<DpResult<CommitResponse>> = std::thread::scope(|scope| {
        let hs: Vec<_> = claimed
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let p = if i % 2 == 0 { &fx.plane } else { &other };
                let ops = complete_ops(c);
                scope.spawn(move || commit_on(p, &format!("done{i}"), ops, Params::new()))
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for r in &results {
        ensure!(committed(r), "child completion: {r:?}");
    }
    // A duplicate completion (new request id, same lease) is refused whole.
    let r = commit_on(
        &fx.plane,
        "done0-again",
        complete_ops(&claimed[0]),
        Params::new(),
    );
    ensure!(!committed(&r), "duplicate completion committed: {r:?}");
    let v = view(fx, pid)?;
    ensure!(
        v.fields.get(&f("pending")) == Some(&Value::Int(0)),
        "counter: {:?}",
        v.fields.get(&f("pending"))
    );
    ensure!(
        body(&v)["wakes"] == json!(["go"]),
        "wakes: {}",
        body(&v)["wakes"]
    );
    let c = must(claim(fx)?, "claim the join")?;
    ensure!(c.item_id == pid, "the join is due");
    Ok(())
}

// ------------------------------------------------------------ send intent

/// The send intent commits with the step only if the address is not
/// suppressed at that instant. Suppression arriving after the claim still
/// stops it; a rejected send leaves the instance leased and unchanged.
fn dsl_send_suppressed(fx: &Fixture) -> T {
    setup(fx)?;
    let suppress = |addr: &str, id: &str| {
        let mut row = row_of(vec![
            ("account", text("acct")),
            ("channel", text("email")),
            ("address", text(addr)),
        ]);
        row.insert(f("reasons"), Value::TextSet(set_of(&["bounce"])));
        commit_on(
            &fx.plane,
            id,
            vec![put_rows("supp", vec![row], RowConflict::Merge)],
            Params::new(),
        )
    };
    let send_ops = |c: &ClaimedItem, addr: &str, intent: &str| {
        let mut effect = NewItem::new(Some(intent), PriorityValue::Int(1));
        effect.body = Some(Body::Json(json!({ "to": addr })));
        effect.fields.insert(f("run"), text("s"));
        vec![
            assert_op(
                "supp",
                row_sel(
                    vec![text("acct"), text("email"), text(addr)],
                    Some(Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::field(
                        "reasons",
                    )))))),
                ),
                0,
                0,
            ),
            fin(finalize_op(c, FinalizeOutcome::Complete)),
            WriteOp::Enqueue {
                queue: o("effects"),
                items: vec![effect],
                on_key_conflict: KeyConflict::Reject,
            },
        ]
    };
    let a = start(fx, "sa", "s", "send", 1)?;
    let b = start(fx, "sb", "s", "send", 2)?;
    let ca = must(claim(fx)?, "claim a")?;
    let cb = must(claim(fx)?, "claim b")?;
    // Suppression lands between the claim and the send.
    ensure!(committed(&suppress("bad@x", "supp-1")), "suppress");
    let r = commit_on(
        &fx.plane,
        "send-a",
        send_ops(&ca, "bad@x", "intent-a"),
        Params::new(),
    );
    ensure!(
        rejected_for(&r, |x| *x == FailureReason::MatchExpectUnmet { matched: 1 }),
        "send to a suppressed address: {r:?}"
    );
    let va = view(fx, a)?;
    ensure!(
        va.state == ItemState::Leased && va.item_version == ca.item_version,
        "a rejected send changed the instance: {va:?}"
    );
    ensure!(effect_count(fx)? == 0, "an effect was enqueued");
    let r = commit_on(
        &fx.plane,
        "send-b",
        send_ops(&cb, "ok@x", "intent-b"),
        Params::new(),
    );
    ensure!(committed(&r), "clean send: {r:?}");
    let again = commit_on(
        &fx.plane,
        "send-b",
        send_ops(&cb, "ok@x", "intent-b"),
        Params::new(),
    );
    ensure!(
        matches!((&r, &again), (Ok(x), Ok(y)) if x == y),
        "send replay differs"
    );
    let r = commit_on(
        &fx.plane,
        "send-b2",
        send_ops(&cb, "ok@x", "intent-b2"),
        Params::new(),
    );
    ensure!(
        !committed(&r),
        "a second send for a finished step committed"
    );
    ensure!(effect_count(fx)? == 1, "effects: {}", effect_count(fx)?);
    ensure!(view(fx, b)?.state == ItemState::Complete, "b not complete");
    Ok(())
}

fn effect_count(fx: &Fixture) -> Result<usize, Fail> {
    let mut n = 0;
    let mut token = None;
    loop {
        let r = ok(
            block_on(fx.plane.read(ReadRequest {
                space: sp(),
                name: ReadName::new("fw_items"),
                version: 1,
                params: params(vec![("queue", text("effects")), ("state", text("pending"))]),
                page_token: token,
            })),
            "fw_items",
        )?;
        n += r.rows.len();
        token = r.next_page_token;
        if token.is_none() {
            return Ok(n);
        }
    }
}

// ------------------------------------------------------------- crash loop

/// A worker that crashes on an instance every time: each lease expires,
/// and after `max_attempts` claims the instance fails instead of looping.
/// A crash after a successful step does not count against the next step.
fn dsl_crash_loop(fx: &Fixture) -> T {
    setup(fx)?;
    let ok_id = start(fx, "steady", "k", "step", 1)?;
    let c = must(claim(fx)?, "claim steady")?;
    ok(
        fx.commit(vec![fin(finalize_op(&c, rearm(now())))]),
        "steady step",
    )?;
    let id = start(fx, "poison", "p", "step", 1)?;
    for i in 1..=3 {
        let c = must(claim_run(fx, "p")?, "claim poison")?;
        ensure!(c.attempt_count == i, "attempt {i}");
        fx.advance(60_000); // the worker died; the lease expires
    }
    let v = view(fx, id)?;
    ensure!(
        v.state == ItemState::Failed
            && v.terminal.as_ref().and_then(|t| t.code.as_deref()) == Some("attempts-exhausted"),
        "poison instance: {v:?}"
    );
    ensure!(claim_run(fx, "p")?.is_none(), "failed instance claimable");
    let c = must(claim_run(fx, "k")?, "claim steady again")?;
    ensure!(
        c.item_id == ok_id && c.attempt_count == 1,
        "re-armed instance carried attempts: {}",
        c.attempt_count
    );
    Ok(())
}

// ----------------------------------------------------------------- cancel

/// Cancelling a run with a chunked `Mutate` while some instances are
/// leased, under each lease policy.
fn dsl_cancel_run(fx: &Fixture) -> T {
    setup(fx)?;
    let cancel = |run: &str,
                  policy: LeasePolicy|
     -> Result<(Vec<CommitResponse>, Option<DpResult<CommitResponse>>), Fail> {
        let mut m = Mutate::new(
            "inst",
            Selection::Where {
                predicate: Expr::eq(Expr::field("run"), Expr::text(run)),
            },
            Action::Transition(Transition::Fail {
                failure_code: "cancelled".into(),
            }),
            2,
        );
        m.lease_policy = policy;
        m.chunk = Some(Chunk::Start);
        let mut steps = vec![];
        loop {
            let r = fx.commit(vec![WriteOp::Mutate(m.clone())]);
            match &r {
                Ok(x) if x.status == CommitStatus::Committed => {
                    let x = x.clone();
                    let next = x.continuation.clone();
                    steps.push(x);
                    match next {
                        Some(c) => m.chunk = Some(Chunk::Continue(c)),
                        None => return Ok((steps, None)),
                    }
                }
                _ => return Ok((steps, Some(r))),
            }
        }
    };
    let mut leased = std::collections::BTreeMap::new();
    let mut ids = std::collections::BTreeMap::new();
    for run in ["skip", "rej", "brk"] {
        let mut v = vec![];
        for i in 0..5 {
            v.push(start(fx, &format!("{run}{i}"), run, "step", i)?);
        }
        let mut l = vec![];
        for _ in 0..2 {
            l.push(must(claim_run(fx, run)?, "claim")?);
        }
        ids.insert(run, v);
        leased.insert(run, l);
    }
    let states = |run: &str| -> Result<Vec<ItemState>, Fail> {
        ids[run]
            .iter()
            .map(|id| view(fx, *id).map(|v| v.state))
            .collect()
    };
    // Skip: pending instances cancelled, leased ones untouched.
    let (steps, err) = cancel("skip", LeasePolicy::Skip)?;
    ensure!(err.is_none(), "Skip: {err:?}");
    let skipped: u64 = steps.iter().map(|s| s.ops[0].skipped_leased).sum();
    ensure!(skipped == 2, "Skip counted {skipped} leased");
    let s = states("skip")?;
    ensure!(
        s.iter().filter(|x| **x == ItemState::Failed).count() == 3
            && s.iter().filter(|x| **x == ItemState::Leased).count() == 2,
        "Skip states: {s:?}"
    );
    // Reject: the step that meets a leased instance rejects whole.
    let (_, err) = cancel("rej", LeasePolicy::Reject)?;
    ensure!(
        matches!(&err, Some(r) if rejected_for(r, |x| *x == FailureReason::LeasedMatch)),
        "Reject: {err:?}"
    );
    let s = states("rej")?;
    ensure!(
        s.iter().filter(|x| **x == ItemState::Leased).count() == 2,
        "Reject touched a leased instance: {s:?}"
    );
    // Break: everything cancelled; the old workers' finalize is stale.
    let (_, err) = cancel("brk", LeasePolicy::Break)?;
    ensure!(err.is_none(), "Break: {err:?}");
    ensure!(
        states("brk")?.iter().all(|x| *x == ItemState::Failed),
        "Break states"
    );
    let r = fx.commit(vec![fin(finalize_op(
        &leased["brk"][0],
        FinalizeOutcome::Complete,
    ))]);
    ensure!(!committed(&r), "a broken lease finalized: {r:?}");
    // Preserve cannot transition; MatchToken needs one key.
    let mut m = Mutate::new(
        "inst",
        Selection::Where {
            predicate: Expr::eq(Expr::field("run"), Expr::text("skip")),
        },
        Action::Transition(Transition::Fail {
            failure_code: "cancelled".into(),
        }),
        5,
    );
    m.lease_policy = LeasePolicy::Preserve;
    let r = fx.commit(vec![WriteOp::Mutate(m.clone())]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "Preserve: {r:?}"
    );
    let tok = leased["skip"][0].lease_token.clone().expect("token");
    m.lease_policy = LeasePolicy::MatchToken(tok.clone());
    let r = fx.commit(vec![WriteOp::Mutate(m.clone())]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "MatchToken on Where: {r:?}"
    );
    m.selection = item_sel(leased["skip"][0].item_id, None);
    let r = fx.commit(vec![WriteOp::Mutate(m)]);
    ensure!(committed(&r), "MatchToken with the holder's token: {r:?}");
    ensure!(
        view(fx, leased["skip"][0].item_id)?.state == ItemState::Failed,
        "MatchToken cancel"
    );
    Ok(())
}

// --------------------------------------------------------------- schedule

/// A schedule entry: a recurring item whose tick starts a run, keyed by the
/// tick time so a replayed or repeated tick starts nothing new. Missed
/// ticks collapse into one.
fn dsl_schedule_tick(fx: &Fixture) -> T {
    setup(fx)?;
    let period = 60_000;
    let mut tick = NewItem::new(Some("sched"), PriorityValue::Int(0));
    tick.fields.insert(f("run"), text("sched"));
    tick.not_before = Some(Schedule::At(UtcMillis(START + period)));
    ok(
        fx.commit(vec![WriteOp::Enqueue {
            queue: o("inst"),
            items: vec![tick],
            on_key_conflict: KeyConflict::Reject,
        }]),
        "schedule",
    )?;
    let fire = |c: &ClaimedItem, rid: &str| {
        let t = fx.clock.now().0;
        let due = (t / period + 1) * period;
        let mut run = new_instance(
            &format!("run-{}", c.not_before.map(|x| x.0).unwrap_or(0)),
            "runs",
            "entry",
            5,
        );
        run.fields.insert(f("run"), text("runs"));
        commit_on(
            &fx.plane,
            rid,
            vec![
                fin(finalize_op(c, rearm(Schedule::At(UtcMillis(due))))),
                WriteOp::Enqueue {
                    queue: o("inst"),
                    items: vec![run],
                    on_key_conflict: KeyConflict::Duplicate,
                },
            ],
            Params::new(),
        )
    };
    ensure!(claim_run(fx, "sched")?.is_none(), "tick before its time");
    fx.advance(period);
    let c = must(claim_run(fx, "sched")?, "first tick")?;
    let r1 = fire(&c, "tick-1");
    ensure!(committed(&r1), "fire: {r1:?}");
    let r1b = fire(&c, "tick-1");
    ensure!(
        matches!((&r1, &r1b), (Ok(a), Ok(b)) if a == b),
        "replayed tick differs"
    );
    // Three periods pass unobserved: one tick, then the next due time.
    fx.advance(3 * period + 5);
    let c = must(claim_run(fx, "sched")?, "late tick")?;
    ensure!(c.attempt_count == 1, "re-armed tick carried attempts");
    ensure!(committed(&fire(&c, "tick-2")), "late fire");
    ensure!(
        claim_run(fx, "sched")?.is_none(),
        "missed ticks fired again"
    );
    let mut runs = 0;
    while claim_run(fx, "runs")?.is_some() {
        runs += 1;
    }
    ensure!(runs == 2, "{runs} runs started for two ticks");
    Ok(())
}

// --------------------------------------------------------------- timeline

/// Each transition appends a timeline row in its own group; rejected groups
/// append nothing; a reader following its cursor sees every transition of
/// the run once, in commit order.
fn dsl_timeline(fx: &Fixture) -> T {
    setup(fx)?;
    let tl = |what: &str, item: Option<ItemId>| {
        put_rows(
            "timeline",
            vec![row_of(vec![
                ("run", text("t")),
                ("what", text(what)),
                ("item", Value::Int(item.map(|i| i.0 as i64).unwrap_or(-1))),
            ])],
            RowConflict::Reject,
        )
    };
    let r = ok(
        fx.commit(vec![
            WriteOp::Enqueue {
                queue: o("inst"),
                items: vec![new_instance("tl", "t", "wait", 1)],
                on_key_conflict: KeyConflict::Reject,
            },
            tl("started", None),
        ]),
        "start",
    )?;
    let id = r.ops[0].entries[0]
        .item_id
        .ok_or_else(|| Fail::Failed("id".into()))?;
    let mut cursor = 0i64;
    let mut seen: Vec<(i64, String)> = vec![];
    let mut follow = |fx: &Fixture| -> T {
        let mut token = None;
        loop {
            let r = ok(
                fx.read_page(
                    "timeline_after",
                    params(vec![("after", Value::Int(cursor))]),
                    token,
                ),
                "follow",
            )?;
            for row in &r.rows {
                if let (Some(Value::Int(n)), Some(Value::Text(w))) =
                    (row.get(&f("seq")), row.get(&f("what")))
                {
                    seen.push((*n, w.clone()));
                }
            }
            token = r.next_page_token;
            if token.is_none() {
                break;
            }
        }
        cursor = seen.last().map(|x| x.0).unwrap_or(cursor);
        Ok(())
    };
    follow(fx)?;
    let c = must(claim(fx)?, "claim")?;
    let mut ops = wait_ops(&c, "t", "c");
    ops.push(tl("waiting", Some(id)));
    ensure!(committed(&fx.commit(ops)), "wait");
    follow(fx)?;
    // A rejected group (duplicate start) appends nothing.
    let r = fx.commit(vec![
        WriteOp::Enqueue {
            queue: o("inst"),
            items: vec![new_instance("tl", "t", "wait", 1)],
            on_key_conflict: KeyConflict::Reject,
        },
        tl("started-again", None),
    ]);
    ensure!(!committed(&r), "duplicate start committed");
    let mut wake = wake_ops();
    wake.push(tl("woken", Some(id)));
    let w = ok(
        fx.commit_with(fx.hdr(), wake, ev_params("t", "c", json!("x")), false),
        "wake",
    )?;
    ensure!(w.status == CommitStatus::Committed, "wake: {w:?}");
    let c = must(claim(fx)?, "claim woken")?;
    ensure!(
        committed(&fx.commit(vec![
            fin(finalize_op(&c, FinalizeOutcome::Complete)),
            tl("done", Some(id)),
        ])),
        "complete"
    );
    follow(fx)?;
    let whats: Vec<&str> = seen.iter().map(|x| x.1.as_str()).collect();
    ensure!(
        whats == ["started", "waiting", "woken", "done"],
        "timeline: {whats:?}"
    );
    let seqs: Vec<i64> = seen.iter().map(|x| x.0).collect();
    ensure!(seqs == vec![1, 2, 3, 4], "sequence numbers: {seqs:?}");
    Ok(())
}

// --------------------------------------------------------------- rate cap

/// A per-contact cap of three sends per window: a counter row per (contact,
/// window), created if absent and incremented only while below the limit,
/// in the same group as the send. The counter cannot be lowered.
fn dsl_rate_cap(fx: &Fixture) -> T {
    setup(fx)?;
    let limit = 3;
    let send = |window: i64, n: usize| {
        let k = format!("c1:{window}");
        let mut inc = Mutate::new(
            "counters",
            row_sel(
                vec![text(&k)],
                Some(Expr::cmp(
                    CmpOp::Lt,
                    Expr::field("count"),
                    Expr::param("limit"),
                )),
            ),
            Action::Set(vec![(
                f("count"),
                Expr::arith(ArithOp::Add, Expr::field("count"), Expr::int(1)),
            )]),
            1,
        );
        inc.expect = Some(MatchExpect { min: 1, max: 1 });
        let mut effect = NewItem::new(Some(&format!("rc-{window}-{n}")), PriorityValue::Int(1));
        effect.fields.insert(f("run"), text("rc"));
        fx.commit_with(
            fx.hdr(),
            vec![
                put_rows(
                    "counters",
                    vec![row_of(vec![
                        ("k", text(&k)),
                        ("window", Value::Int(window)),
                        ("count", Value::Int(0)),
                    ])],
                    RowConflict::Skip,
                ),
                WriteOp::Mutate(inc),
                WriteOp::Enqueue {
                    queue: o("effects"),
                    items: vec![effect],
                    on_key_conflict: KeyConflict::Reject,
                },
            ],
            params(vec![("limit", Value::Int(limit))]),
            false,
        )
    };
    for n in 0..3 {
        let r = send(1, n);
        ensure!(committed(&r), "send {n}: {r:?}");
    }
    let r = send(1, 3);
    ensure!(
        rejected_for(&r, |x| *x == FailureReason::MatchExpectUnmet { matched: 0 }),
        "fourth send in the window: {r:?}"
    );
    ensure!(committed(&send(2, 0)), "first send in the next window");
    let mut lower = Mutate::new(
        "counters",
        row_sel(vec![text("c1:1")], None),
        Action::Set(vec![(f("count"), Expr::int(0))]),
        1,
    );
    lower.expect = Some(MatchExpect { min: 1, max: 1 });
    let r = fx.commit(vec![WriteOp::Mutate(lower)]);
    ensure!(
        rejected_for(&r, |x| matches!(
            x,
            FailureReason::Entry(EntryStatus::MergeRuleViolated { .. })
        )),
        "lowering the counter: {r:?}"
    );
    let row = fx
        .get_row_key("counters", vec![text("c1:1")])
        .ok_or_else(|| Fail::Failed("counter missing".into()))?;
    ensure!(row.fields[&f("count")] == Value::Int(3), "counter: {row:?}");
    ensure!(effect_count(fx)? == 4, "effects: {}", effect_count(fx)?);
    Ok(())
}
