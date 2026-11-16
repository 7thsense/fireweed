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
    ("DSL-15", dsl_suppression_copy),
    ("DSL-16", dsl_duplicate_admission),
    ("DSL-17", dsl_effect_outcome_unknown),
    ("DSL-18", dsl_crash_before_provider_call),
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
        fields: vec![
            FieldDecl::stored("run", Text, true, IndexHint::Lookup),
            FieldDecl::stored("outcome", Text, true, IndexHint::Lookup),
        ],
    });
    // Admission: keyed by (run, subject); a terminal item stops being
    // addressable after a minute and releases its key after two.
    let mut admit = QueueSpec::basic(PriorityKind::Int64);
    admit.terminal_retention_ms = 60_000;
    admit.client_item_key_retention_ms = 120_000;
    s.queues.push(QueueDecl {
        name: o("admit"),
        spec: admit,
        body: BodyKind::Json,
        fields: vec![
            FieldDecl::stored("run", Text, true, IndexHint::Lookup),
            FieldDecl::stored("subject", Text, true, IndexHint::None),
        ],
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
        matches!((&r, &again), (Ok(x), Ok(y)) if same_replay(x, y)),
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
        matches!((&r1, &r1b), (Ok(a), Ok(b)) if same_replay(a, b)),
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

// ------------------------------------------------ suppression across spaces

fn s_key() -> SpaceKey {
    SpaceKey::new("tenant", "acct")
}

fn c_key() -> SpaceKey {
    SpaceKey::new("tenant", "camp")
}

fn commit_in(
    p: &Arc<dyn DataPlane>,
    space: SpaceKey,
    id: &str,
    ops: Vec<WriteOp>,
    ps: Params,
) -> DpResult<CommitResponse> {
    block_on(p.commit(CommitRequest {
        header: WriteHeader::new(space, id),
        ops,
        params: ps,
        dry_run: false,
    }))
}

fn row_in(p: &Arc<dyn DataPlane>, space: SpaceKey, table: &str, addr: &str) -> Option<RowView> {
    let r = block_on(p.get(GetRequest {
        space,
        targets: vec![GetTarget::Row {
            table: o(table),
            key: vec![text(addr)],
        }],
    }))
    .ok()?;
    match r.results.into_iter().next().flatten() {
        Some(GetResult::Row(v)) => Some(v),
        _ => None,
    }
}

fn reasons_of(row: &Option<RowView>) -> BTreeSet<String> {
    match row.as_ref().and_then(|r| r.fields.get(&f("reasons"))) {
        Some(Value::TextSet(s)) => s.clone(),
        _ => BTreeSet::new(),
    }
}

fn int_of(row: &Option<RowView>, field: &str) -> Option<i64> {
    match row.as_ref().and_then(|r| r.fields.get(&f(field))) {
        Some(Value::Int(n)) => Some(*n),
        _ => None,
    }
}

/// S, the account space: the authoritative `supp` table and `supp_log`, a
/// sequenced table whose numbers order every suppression and lift.
/// C, the campaign space: `supp_copy` and the `sends` queue.
fn two_spaces(fx: &Fixture) -> T {
    use ScalarType::*;
    let apply = |space: SpaceKey, schema: SpaceSchema| {
        block_on(fx.plane.apply_schema(ApplySchemaRequest {
            space,
            schema,
            expected_version: None,
            min_durability: None,
        }))
    };
    let s = SpaceSchema {
        version: 1,
        request_id_retention_ms: 600_000,
        queues: vec![],
        tables: vec![
            TableDecl {
                name: o("supp"),
                key: vec![f("address")],
                fields: vec![
                    FieldDecl::stored("address", Text, false, IndexHint::None),
                    FieldDecl::stored("reasons", TextSet, true, IndexHint::None)
                        .with_merge(MergeRule::AddOnly),
                ],
                expiry: None,
                key_assignment: KeyAssignment::Caller,
            },
            TableDecl {
                name: o("supp_log"),
                key: vec![f("seq")],
                fields: vec![
                    FieldDecl::stored("seq", Int, false, IndexHint::Range),
                    FieldDecl::stored("address", Text, true, IndexHint::None),
                    FieldDecl::stored("kind", Text, true, IndexHint::None),
                    FieldDecl::stored("reason", Text, true, IndexHint::None),
                ],
                expiry: None,
                key_assignment: KeyAssignment::Sequence,
            },
        ],
        reads: vec![ReadDecl {
            name: ReadName::new("log_after"),
            version: 1,
            source: o("supp_log"),
            params: vec![(ParamName::new("after"), Int)],
            filter: Expr::cmp(CmpOp::Gt, Expr::field("seq"), Expr::param("after")),
            output: ReadOutput::Rows {
                fields: vec![f("seq")],
            },
            order: vec![(f("seq"), Direction::Asc)],
            max_rows: 100,
        }],
    };
    ok(apply(s_key(), s), "S schema")?;
    let mut spec = QueueSpec::basic(PriorityKind::Int64);
    spec.gates = Some(GatePolicy {
        max_gate_keys_per_item: 4,
        max_gates_per_request: 16,
    });
    let c = SpaceSchema {
        version: 1,
        request_id_retention_ms: 600_000,
        queues: vec![QueueDecl {
            name: o("sends"),
            spec,
            body: BodyKind::Json,
            fields: vec![FieldDecl::stored("addr", Text, true, IndexHint::Lookup)],
        }],
        tables: vec![TableDecl {
            name: o("supp_copy"),
            key: vec![f("address")],
            fields: vec![
                FieldDecl::stored("address", Text, false, IndexHint::None),
                FieldDecl::stored("reasons", TextSet, true, IndexHint::None)
                    .with_merge(MergeRule::AddOnly),
                FieldDecl::stored("lift_seq", Int, true, IndexHint::None)
                    .with_merge(MergeRule::NonDecreasing),
                FieldDecl::stored("supp_max", Int, true, IndexHint::None)
                    .with_merge(MergeRule::NonDecreasing),
            ],
            expiry: None,
            key_assignment: KeyAssignment::Caller,
        }],
        reads: vec![],
    };
    ok(apply(c_key(), c), "C schema")?;
    Ok(())
}

fn s_number(r: &CommitResponse) -> Result<i64, Fail> {
    match r.ops[0].entries.first().and_then(|e| e.row_key.as_deref()) {
        Some([Value::Int(n)]) if r.status == CommitStatus::Committed => Ok(*n),
        _ => Err(Fail::Failed(format!("no S number: {r:?}"))),
    }
}

/// A suppression in S: numbered by `supp_log`, then unioned into `supp`.
fn s_suppress(fx: &Fixture, addr: &str, reason: &str) -> Result<i64, Fail> {
    let r = ok(
        commit_in(
            &fx.plane,
            s_key(),
            &fx.rid(),
            vec![
                put_rows(
                    "supp_log",
                    vec![row_of(vec![
                        ("address", text(addr)),
                        ("kind", text("supp")),
                        ("reason", text(reason)),
                    ])],
                    RowConflict::Reject,
                ),
                put_rows(
                    "supp",
                    vec![row_of(vec![("address", text(addr))])],
                    RowConflict::Skip,
                ),
                WriteOp::Mutate(Mutate::new(
                    "supp",
                    row_sel(vec![text(addr)], None),
                    Action::Set(vec![(
                        f("reasons"),
                        Expr::SetAdd {
                            set: Box::new(Expr::field("reasons")),
                            values: vec![Expr::text(reason)],
                        },
                    )]),
                    1,
                )),
            ],
            Params::new(),
        ),
        "S suppress",
    )?;
    s_number(&r)
}

/// The lift in S, authoritative first: numbered, then an override clear.
fn s_lift(fx: &Fixture, addr: &str) -> Result<i64, Fail> {
    let mut clear = Mutate::new(
        "supp",
        row_sel(vec![text(addr)], None),
        Action::Set(vec![(f("reasons"), Expr::null(ScalarType::TextSet))]),
        1,
    );
    clear.override_merge_rules = true;
    let r = ok(
        commit_in(
            &fx.plane,
            s_key(),
            &fx.rid(),
            vec![
                put_rows(
                    "supp_log",
                    vec![row_of(vec![
                        ("address", text(addr)),
                        ("kind", text("lift")),
                    ])],
                    RowConflict::Reject,
                ),
                WriteOp::Mutate(clear),
            ],
            Params::new(),
        ),
        "S lift",
    )?;
    s_number(&r)
}

fn copy_seed_op(addr: &str) -> WriteOp {
    put_rows(
        "supp_copy",
        vec![row_of(vec![
            ("address", text(addr)),
            ("lift_seq", Value::Int(0)),
            ("supp_max", Value::Int(0)),
        ])],
        RowConflict::Skip,
    )
}

/// A suppression fanned out to C with S's number `n`. It applies only if
/// `n` is above the copy's `lift_seq`; `provider_at` is carried but never
/// used for ordering.
fn c_apply_supp(
    p: &Arc<dyn DataPlane>,
    id: &str,
    addr: &str,
    reason: &str,
    n: i64,
    provider_at: i64,
) -> DpResult<CommitResponse> {
    let add = Mutate::new(
        "supp_copy",
        row_sel(
            vec![text(addr)],
            Some(Expr::cmp(
                CmpOp::Lt,
                Expr::field("lift_seq"),
                Expr::param("n"),
            )),
        ),
        Action::Set(vec![(
            f("reasons"),
            Expr::SetAdd {
                set: Box::new(Expr::field("reasons")),
                values: vec![Expr::param("reason")],
            },
        )]),
        1,
    );
    let high = Mutate::new(
        "supp_copy",
        row_sel(
            vec![text(addr)],
            Some(Expr::cmp(
                CmpOp::Lt,
                Expr::field("supp_max"),
                Expr::param("n"),
            )),
        ),
        Action::Set(vec![(f("supp_max"), Expr::param("n"))]),
        1,
    );
    commit_in(
        p,
        c_key(),
        id,
        vec![
            copy_seed_op(addr),
            WriteOp::Mutate(add),
            WriteOp::Mutate(high),
        ],
        params(vec![
            ("n", Value::Int(n)),
            ("reason", text(reason)),
            ("provider_at", Value::Timestamp(UtcMillis(provider_at))),
        ]),
    )
}

/// The lift marker with S's number `l`. The copy clears (by override) only
/// if `l` is above both its `lift_seq` and every suppression it holds;
/// otherwise it stays suppressed (fail closed). `lift_seq` then advances
/// to `l`, so older suppressions arriving later are dropped.
fn c_apply_lift(p: &Arc<dyn DataPlane>, id: &str, addr: &str, l: i64) -> DpResult<CommitResponse> {
    let below = |field: &str| Expr::cmp(CmpOp::Lt, Expr::field(field), Expr::param("l"));
    let mut clear = Mutate::new(
        "supp_copy",
        row_sel(
            vec![text(addr)],
            Some(Expr::And(vec![below("lift_seq"), below("supp_max")])),
        ),
        Action::Set(vec![
            (f("reasons"), Expr::null(ScalarType::TextSet)),
            (f("lift_seq"), Expr::param("l")),
        ]),
        1,
    );
    clear.override_merge_rules = true;
    let advance = Mutate::new(
        "supp_copy",
        row_sel(vec![text(addr)], Some(below("lift_seq"))),
        Action::Set(vec![(f("lift_seq"), Expr::param("l"))]),
        1,
    );
    commit_in(
        p,
        c_key(),
        id,
        vec![
            copy_seed_op(addr),
            WriteOp::Mutate(clear),
            WriteOp::Mutate(advance),
        ],
        params(vec![("l", Value::Int(l))]),
    )
}

fn c_claim(fx: &Fixture) -> Result<Option<ClaimedItem>, Fail> {
    let r = ok(
        block_on(fx.plane.claim(ClaimRequest::new(
            WriteHeader::new(c_key(), fx.rid()),
            "sends",
            "w",
            1,
            600_000,
        ))),
        "claim send",
    )?;
    Ok(r.items.into_iter().next())
}

fn c_enqueue_send(fx: &Fixture, key: &str, addr: &str, prio: i64) -> T {
    let mut it = NewItem::new(Some(key), PriorityValue::Int(prio));
    it.fields.insert(f("addr"), text(addr));
    it.gate_keys.insert("active".into());
    let r = commit_in(
        &fx.plane,
        c_key(),
        &fx.rid(),
        vec![WriteOp::Enqueue {
            queue: o("sends"),
            items: vec![it],
            on_key_conflict: KeyConflict::Reject,
        }],
        Params::new(),
    );
    ensure!(committed(&r), "enqueue send {key}: {r:?}");
    Ok(())
}

/// The send: completes the claimed send only if C's copy is not
/// suppressed at that instant.
fn c_send(fx: &Fixture, c: &ClaimedItem) -> DpResult<CommitResponse> {
    let addr = c.fields.get(&f("addr")).cloned().unwrap_or(text(""));
    commit_in(
        &fx.plane,
        c_key(),
        &fx.rid(),
        vec![
            assert_op(
                "supp_copy",
                row_sel(
                    vec![addr],
                    Some(Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::field(
                        "reasons",
                    )))))),
                ),
                0,
                0,
            ),
            WriteOp::Finalize {
                queue: o("sends"),
                entries: vec![FinalizeEntry::new(lease_of(c), FinalizeOutcome::Complete)],
            },
        ],
        Params::new(),
    )
}

fn blocked(r: &DpResult<CommitResponse>) -> bool {
    rejected_for(r, |x| *x == FailureReason::MatchExpectUnmet { matched: 1 })
}

/// Account-level suppression copied into a campaign space, ordered by S's
/// own numbers: activation seeding (f), out-of-order fan-out (a), sends
/// blocked by the copy (b), authoritative-first lift (c), fail closed
/// between S's lift and the copy (d), late and skewed copies dropped while
/// a real post-lift bounce re-suppresses (e), and a post-lift suppression
/// that reaches the copy before the lift marker.
fn dsl_suppression_copy(fx: &Fixture) -> T {
    two_spaces(fx)?;
    let other = fx.open_peer().unwrap_or_else(|| fx.plane.clone());
    // (f) Sends wait behind a closed activation gate.
    let gate = |state: GateState| WriteOp::SetGates {
        queue: o("sends"),
        gates: vec![("active".into(), state)],
    };
    ensure!(
        committed(&commit_in(
            &fx.plane,
            c_key(),
            &fx.rid(),
            vec![gate(GateState::Blocked)],
            Params::new()
        )),
        "close gate"
    );
    c_enqueue_send(fx, "s-x", "x@y", 1)?;
    c_enqueue_send(fx, "s-z", "z@y", 2)?;
    ensure!(
        c_claim(fx)?.is_none(),
        "a send was claimable before activation"
    );
    let n1 = s_suppress(fx, "x@y", "bounce")?;
    // Activation: snapshot S (its highest number H, then its rows), seed
    // the copy at H and open the gate in the same group.
    let mut h = 0;
    let mut token = None;
    loop {
        let r = ok(
            block_on(fx.plane.read(ReadRequest {
                space: s_key(),
                name: ReadName::new("log_after"),
                version: 1,
                params: params(vec![("after", Value::Int(0))]),
                page_token: token,
            })),
            "S log",
        )?;
        for row in &r.rows {
            if let Some(Value::Int(n)) = row.get(&f("seq")) {
                h = h.max(*n);
            }
        }
        token = r.next_page_token;
        if token.is_none() {
            break;
        }
    }
    let mut seed = vec![];
    for addr in ["x@y", "z@y"] {
        let mut row = row_of(vec![
            ("address", text(addr)),
            ("lift_seq", Value::Int(h)),
            ("supp_max", Value::Int(h)),
        ]);
        let rs = reasons_of(&row_in(&fx.plane, s_key(), "supp", addr));
        if !rs.is_empty() {
            row.insert(f("reasons"), Value::TextSet(rs));
        }
        seed.push(row);
    }
    let r = commit_in(
        &fx.plane,
        c_key(),
        &fx.rid(),
        vec![
            put_rows("supp_copy", seed, RowConflict::Reject),
            gate(GateState::Open),
        ],
        Params::new(),
    );
    ensure!(committed(&r) && h >= n1, "activation: {r:?}");
    let cx = must(c_claim(fx)?, "claim x")?;
    ensure!(
        blocked(&c_send(fx, &cx)),
        "seeded suppression did not block x@y"
    );
    let cz = must(c_claim(fx)?, "claim z")?;
    ensure!(committed(&c_send(fx, &cz)), "unsuppressed z@y blocked");
    // The fan-out of n1 arrives after seeding: already reflected, dropped.
    ok(
        c_apply_supp(&fx.plane, "f-n1", "x@y", "bounce", n1, START),
        "late n1",
    )?;
    ensure!(
        reasons_of(&row_in(&fx.plane, c_key(), "supp_copy", "x@y")) == set_of(&["bounce"]),
        "x@y copy"
    );
    // (a) Two suppressions fan out in reverse order, concurrently.
    let n2 = s_suppress(fx, "z@y", "stop")?;
    let n3 = s_suppress(fx, "z@y", "complaint")?;
    let (r3, r2) = std::thread::scope(|scope| {
        let a = scope.spawn(|| c_apply_supp(&fx.plane, "f-n3", "z@y", "complaint", n3, START));
        let b = scope.spawn(|| c_apply_supp(&other, "f-n2", "z@y", "stop", n2, START));
        (a.join().unwrap(), b.join().unwrap())
    });
    ensure!(committed(&r3) && committed(&r2), "fan-out: {r3:?} {r2:?}");
    let copy = row_in(&fx.plane, c_key(), "supp_copy", "z@y");
    ensure!(
        reasons_of(&copy) == reasons_of(&row_in(&fx.plane, s_key(), "supp", "z@y"))
            && reasons_of(&copy) == set_of(&["stop", "complaint"])
            && int_of(&copy, "supp_max") == Some(n3),
        "copy did not converge: {copy:?}"
    );
    // (b) A send to z@y is blocked while the copy is suppressed.
    c_enqueue_send(fx, "s-z2", "z@y", 3)?;
    let cz2 = must(c_claim(fx)?, "claim z2")?;
    ensure!(blocked(&c_send(fx, &cz2)), "send to suppressed z@y");
    // (c), (d) The lift lands in S first; until the copy applies it, the
    // send stays blocked, and C cannot clear the copy on its own.
    let l = s_lift(fx, "z@y")?;
    ensure!(
        reasons_of(&row_in(&fx.plane, s_key(), "supp", "z@y")).is_empty(),
        "S lift"
    );
    ensure!(blocked(&c_send(fx, &cz2)), "send between S lift and copy");
    let plain = commit_in(
        &fx.plane,
        c_key(),
        &fx.rid(),
        vec![WriteOp::Mutate(Mutate::new(
            "supp_copy",
            row_sel(vec![text("z@y")], None),
            Action::Set(vec![(f("reasons"), Expr::null(ScalarType::TextSet))]),
            1,
        ))],
        Params::new(),
    );
    ensure!(
        rejected_for(&plain, |x| matches!(
            x,
            FailureReason::Entry(EntryStatus::MergeRuleViolated { .. })
        )),
        "plain clear in C: {plain:?}"
    );
    ensure!(
        committed(&c_apply_lift(&fx.plane, "f-l", "z@y", l)),
        "lift marker"
    );
    let copy = row_in(&fx.plane, c_key(), "supp_copy", "z@y");
    ensure!(
        reasons_of(&copy).is_empty() && int_of(&copy, "lift_seq") == Some(l),
        "after the marker: {copy:?}"
    );
    ensure!(committed(&c_send(fx, &cz2)), "send after the lift");
    // (e) A late copy of a pre-lift suppression, even with a provider time
    // skewed far after the lift, is dropped; a real post-lift bounce, with
    // a higher S number, re-suppresses.
    ok(
        c_apply_supp(&fx.plane, "f-n2-late", "z@y", "stop", n2, START + FAR),
        "late n2",
    )?;
    ensure!(
        reasons_of(&row_in(&fx.plane, c_key(), "supp_copy", "z@y")).is_empty(),
        "a pre-lift copy re-suppressed"
    );
    let n4 = s_suppress(fx, "z@y", "bounce")?;
    ensure!(n4 > l, "S numbers out of order");
    ok(
        c_apply_supp(&fx.plane, "f-n4", "z@y", "bounce", n4, START),
        "n4",
    )?;
    c_enqueue_send(fx, "s-z3", "z@y", 4)?;
    let cz3 = must(c_claim(fx)?, "claim z3")?;
    ensure!(
        blocked(&c_send(fx, &cz3)),
        "post-lift bounce did not re-suppress"
    );
    // A post-lift suppression that reaches the copy before the lift marker:
    // the marker does not clear it (fail closed); S agrees it is suppressed.
    let m5 = s_suppress(fx, "w@y", "stop")?;
    let l2 = s_lift(fx, "w@y")?;
    let m6 = s_suppress(fx, "w@y", "bounce")?;
    ok(
        c_apply_supp(&fx.plane, "f-m5", "w@y", "stop", m5, START),
        "m5",
    )?;
    ok(
        c_apply_supp(&fx.plane, "f-m6", "w@y", "bounce", m6, START),
        "m6",
    )?;
    ensure!(
        committed(&c_apply_lift(&fx.plane, "f-l2", "w@y", l2)),
        "marker l2"
    );
    let copy = row_in(&fx.plane, c_key(), "supp_copy", "w@y");
    ensure!(
        !reasons_of(&copy).is_empty()
            && !reasons_of(&row_in(&fx.plane, s_key(), "supp", "w@y")).is_empty()
            && int_of(&copy, "lift_seq") == Some(l2),
        "reversed lift: {copy:?}"
    );
    Ok(())
}

// --------------------------------------------------------------- admission

fn admit_ops(subjects: &[&str]) -> Vec<WriteOp> {
    let items = subjects
        .iter()
        .map(|s| {
            let mut it = NewItem::new(Some(&format!("adm-{s}")), PriorityValue::Int(1));
            it.fields.insert(f("run"), text("adm"));
            it.fields.insert(f("subject"), text(s));
            it
        })
        .collect();
    vec![WriteOp::Enqueue {
        queue: o("admit"),
        items,
        on_key_conflict: KeyConflict::Duplicate,
    }]
}

/// A batch admitting one subject twice admits it once. Re-entry `never`:
/// after the instance completes, the subject stays a duplicate while its
/// key is retained, including after the item stops being addressable, and
/// is admitted again exactly at `client_item_key_retention_ms`.
fn dsl_duplicate_admission(fx: &Fixture) -> T {
    setup(fx)?;
    let r = ok(fx.commit(admit_ops(&["s1", "s2", "s1"])), "admit")?;
    let st: Vec<_> = r.ops[0].entries.iter().map(|e| e.status.clone()).collect();
    ensure!(
        st == [
            EntryStatus::Accepted,
            EntryStatus::Accepted,
            EntryStatus::Duplicate
        ],
        "batch statuses: {st:?}"
    );
    let s1 = r.ops[0].entries[0]
        .item_id
        .ok_or_else(|| Fail::Failed("id".into()))?;
    ensure!(
        r.ops[0].entries[2].item_id == Some(s1),
        "duplicate entry names another item: {:?}",
        r.ops[0].entries[2]
    );
    let mut req = ClaimRequest::new(fx.hdr(), "admit", "w", 2, 60_000);
    req.max_items = 2;
    let c = ok(fx.claim_req(req), "claim")?;
    ensure!(c.items.len() == 2, "admitted {} instances", c.items.len());
    let done = c
        .items
        .iter()
        .find(|i| i.item_id == s1)
        .ok_or_else(|| Fail::Failed("s1 not claimed".into()))?;
    ok(
        fx.commit(vec![WriteOp::Finalize {
            queue: o("admit"),
            entries: vec![FinalizeEntry::new(
                lease_of(done),
                FinalizeOutcome::Complete,
            )],
        }]),
        "complete",
    )?;
    let dup = |fx: &Fixture| -> Result<EntryStatus, Fail> {
        let r = ok(fx.commit(admit_ops(&["s1"])), "re-admit")?;
        Ok(r.ops[0].entries[0].status.clone())
    };
    ensure!(
        dup(fx)? == EntryStatus::Duplicate,
        "re-entry after completion"
    );
    fx.advance(60_000);
    ensure!(
        fx.get_in("admit", s1).is_none(),
        "terminal item still addressable"
    );
    ensure!(
        dup(fx)? == EntryStatus::Duplicate,
        "re-entry on a tombstone"
    );
    fx.advance(59_999);
    ensure!(
        dup(fx)? == EntryStatus::Duplicate,
        "re-entry before key retention"
    );
    fx.advance(1);
    let r = ok(fx.commit(admit_ops(&["s1"])), "after retention")?;
    let e = &r.ops[0].entries[0];
    ensure!(
        e.status == EntryStatus::Accepted && e.item_id.is_some_and(|id| id != s1),
        "admission at key retention: {e:?}"
    );
    Ok(())
}

// ----------------------------------------------------------------- effects

fn intent_ops(key: &str) -> Vec<WriteOp> {
    let mut e = NewItem::new(Some(key), PriorityValue::Int(1));
    e.body = Some(Body::Json(json!({ "to": "a@y", "template": "t1" })));
    e.fields.insert(f("run"), text("fx"));
    vec![WriteOp::Enqueue {
        queue: o("effects"),
        items: vec![e],
        on_key_conflict: KeyConflict::Reject,
    }]
}

fn effects_with_key(fx: &Fixture, key: &str) -> Result<usize, Fail> {
    let r = ok(
        block_on(fx.plane.read(ReadRequest {
            space: sp(),
            name: ReadName::new("fw_items"),
            version: 1,
            params: params(vec![("queue", text("effects")), ("key_prefix", text(key))]),
            page_token: None,
        })),
        "fw_items",
    )?;
    Ok(r.rows.len())
}

fn claim_effect(
    fx: &Fixture,
    filter: Option<Expr>,
    lease: u64,
) -> Result<Option<ClaimedItem>, Fail> {
    let mut req = ClaimRequest::new(fx.hdr(), "effects", "w", 1, lease);
    req.filter = filter;
    Ok(ok(fx.claim_req(req), "claim effect")?
        .items
        .into_iter()
        .next())
}

fn effect_fin(c: &ClaimedItem, outcome: FinalizeOutcome, set: Vec<(&str, Expr)>) -> WriteOp {
    let mut e = FinalizeEntry::new(lease_of(c), outcome);
    e.set = set.into_iter().map(|(n, x)| (f(n), x)).collect();
    WriteOp::Finalize {
        queue: o("effects"),
        entries: vec![e],
    }
}

/// The provider call times out; the worker records the outcome as unknown
/// and parks the effect for reconciliation. The same effect item, with the
/// body fixed at intent time, is reconciled exactly once.
fn dsl_effect_outcome_unknown(fx: &Fixture) -> T {
    setup(fx)?;
    let intent = ok(fx.commit(intent_ops("eff-1")), "intent")?;
    ensure!(
        intent.status == CommitStatus::Committed,
        "intent: {intent:?}"
    );
    let id = intent.ops[0].entries[0]
        .item_id
        .ok_or_else(|| Fail::Failed("no effect id".into()))?;
    let c = must(claim_effect(fx, None, 60_000)?, "claim effect")?;
    let body0 = c.body.clone();
    // Timeout: unknown outcome, retry later through reconciliation.
    let h = fx.hdr();
    let ops = vec![effect_fin(
        &c,
        FinalizeOutcome::Retry {
            not_before: Schedule::After { delay_ms: 1000 },
            priority: None,
        },
        vec![("outcome", Expr::text("unknown"))],
    )];
    let t1 = ok(
        fx.commit_with(h.clone(), ops.clone(), Params::new(), false),
        "unknown",
    )?;
    ensure!(
        t1.status == CommitStatus::Committed,
        "record unknown: {t1:?}"
    );
    let t1b = ok(fx.commit_with(h, ops, Params::new(), false), "replay")?;
    ensure!(same_replay(&t1, &t1b), "replayed transition differs");
    let again = ok(
        fx.commit(
            intent_ops("eff-1")
                .into_iter()
                .map(|op| match op {
                    WriteOp::Enqueue { queue, items, .. } => WriteOp::Enqueue {
                        queue,
                        items,
                        on_key_conflict: KeyConflict::Duplicate,
                    },
                    other => other,
                })
                .collect(),
        ),
        "re-enqueue",
    )?;
    ensure!(
        again.ops[0].entries[0].status == EntryStatus::Duplicate
            && again.ops[0].entries[0].item_id == Some(id),
        "re-enqueue: {:?}",
        again.ops[0].entries[0]
    );
    ensure!(effects_with_key(fx, "eff-1")? == 1, "a second effect item");
    fx.advance(1000);
    let unknown = Expr::eq(Expr::field("outcome"), Expr::text("unknown"));
    let r = must(claim_effect(fx, Some(unknown), 60_000)?, "reconcile claim")?;
    ensure!(
        r.item_id == id
            && r.client_item_key == ClientItemKey::new("eff-1")
            && r.body == body0
            && r.attempt_count == 2,
        "reconciliation sees another effect: {r:?}"
    );
    let done = |rid: &str| {
        fx.commit_with(
            WriteHeader::new(sp(), rid),
            vec![effect_fin(
                &r,
                FinalizeOutcome::Complete,
                vec![("outcome", Expr::text("sent"))],
            )],
            Params::new(),
            false,
        )
    };
    ensure!(committed(&done("rec-1")), "reconcile");
    ensure!(!committed(&done("rec-2")), "reconciled twice");
    let v = fx
        .get_in("effects", id)
        .ok_or_else(|| Fail::Failed("effect missing".into()))?;
    ensure!(
        v.state == ItemState::Complete
            && v.fields.get(&f("outcome")) == Some(&text("sent"))
            && v.body == body0,
        "final effect: {v:?}"
    );
    ensure!(
        effects_with_key(fx, "eff-1")? == 1,
        "effects after reconcile"
    );
    Ok(())
}

/// The intent commits at the strongest level offered; the worker crashes
/// before calling the provider. The same effect is redelivered with one
/// more attempt, and replaying the intent creates nothing new.
fn dsl_crash_before_provider_call(fx: &Fixture) -> T {
    setup(fx)?;
    let max = fx.caps().max_durability;
    let mut h = fx.hdr();
    h.min_durability = Some(max);
    let intent = ok(
        fx.commit_with(h.clone(), intent_ops("eff-2"), Params::new(), false),
        "intent",
    )?;
    ensure!(
        intent.status == CommitStatus::Committed && intent.durability.is_some_and(|d| d >= max),
        "intent durability: {intent:?}"
    );
    let id = intent.ops[0].entries[0]
        .item_id
        .ok_or_else(|| Fail::Failed("no effect id".into()))?;
    let c1 = must(claim_effect(fx, None, 1000)?, "first claim")?;
    fx.advance(1000); // crashed before the provider call; the lease expires
    let c2 = must(claim_effect(fx, None, 1000)?, "second claim")?;
    ensure!(
        c2.item_id == id
            && c2.client_item_key == c1.client_item_key
            && c2.body == c1.body
            && c2.attempt_count == c1.attempt_count + 1,
        "redelivery: {c1:?} then {c2:?}"
    );
    let replay = ok(
        fx.commit_with(h, intent_ops("eff-2"), Params::new(), false),
        "replay",
    )?;
    ensure!(same_replay(&intent, &replay), "replayed intent differs");
    ensure!(
        effects_with_key(fx, "eff-2")? == 1,
        "replay created an effect"
    );
    let v = fx
        .get_in("effects", id)
        .ok_or_else(|| Fail::Failed("effect missing".into()))?;
    ensure!(
        v.state == ItemState::Leased && v.attempt_count == 2,
        "the crash exhausted the effect: {v:?}"
    );
    Ok(())
}
