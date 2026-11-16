//! Rule-pinning tests for the revision 7 rulings: lease expiry bounded by
//! `max_attempts`, `Assert`, field merge rules, `Sequence` tables, and the
//! tests that need a second instance or concurrency rather than fault hooks.

use std::collections::BTreeMap;
use std::sync::Arc;

use fireweed_interface::*;
use futures::executor::block_on;

use crate::ensure;
use crate::support::*;

type Test = fn(&Fixture) -> T;

pub const ALL: &[(&str, Test)] = &[
    ("LEASE-MAX", lease_max),
    ("ASSERT-1", assert_1),
    ("ASSERT-2", assert_2),
    ("ASSERT-3", assert_3),
    ("MERGE-1", merge_1),
    ("MERGE-2", merge_2),
    ("MERGE-3", merge_3),
    ("MERGE-4", merge_4),
    ("SEQ-1", seq_1),
    ("SEQ-2", seq_2),
    ("SEQ-3", seq_3),
    ("SEQ-4", seq_4),
    ("REC-5", rec_5),
    ("MULTI-1", multi_1),
    ("TIME-3", time_3),
    ("PG-1", pg_1),
    ("PG-2", time_3),
    ("PG-3", pg_3),
];

fn lit(v: Value) -> Expr {
    Expr::Lit(v)
}

fn bx(e: Expr) -> Box<Expr> {
    Box::new(e)
}

fn ts(ms: i64) -> Value {
    Value::Timestamp(UtcMillis(ms))
}

fn kv_row(k: i64) -> Vec<Value> {
    vec![Value::Int(k)]
}

fn ctr(k: &str) -> Vec<Value> {
    vec![text(k)]
}

fn set_op(table: &str, key: Vec<Value>, sets: Vec<(&str, Expr)>) -> Mutate {
    Mutate::new(
        table,
        row_sel(key, None),
        Action::Set(sets.into_iter().map(|(n, e)| (f(n), e)).collect()),
        1,
    )
}

// ------------------------------------------------------------- lease bound

/// Lease expiry returns the item to pending until its claims have used
/// `max_attempts`; the expiry after that fails it, at the expiry instant.
fn lease_max(fx: &Fixture) -> T {
    let max = 2; // base schema
    let id = fx.push("lm", 1);
    for attempt in 1..=max {
        let c = ok(fx.claim(1, 1000), "claim")?;
        ensure!(
            c.items.len() == 1 && c.items[0].attempt_count == attempt,
            "claim {attempt}: {:?}",
            c.items
        );
        fx.advance(1000);
        let v = fx
            .get_item(id, true)
            .ok_or_else(|| Fail::Failed("item missing".into()))?;
        if attempt < max {
            ensure!(
                v.state == ItemState::Pending && v.attempt_count == attempt,
                "after expiry {attempt}: {v:?}"
            );
        } else {
            let term = v
                .terminal
                .clone()
                .ok_or_else(|| Fail::Failed(format!("not terminal: {v:?}")))?;
            ensure!(
                v.state == ItemState::Failed
                    && term.code.as_deref() == Some("attempts-exhausted")
                    && term.terminal_at == UtcMillis(START + 2000),
                "after the last expiry: {v:?}"
            );
        }
    }
    let c = ok(fx.claim(1, 1000), "claim after failure")?;
    ensure!(c.items.is_empty(), "a failed item was claimed: {c:?}");
    Ok(())
}

// ------------------------------------------------------------------ assert

/// A passing `Assert` changes nothing: no version, no expiry refresh.
fn assert_1(fx: &Fixture) -> T {
    let r = ok(
        fx.commit(vec![put_rows(
            "strict",
            vec![row_of(vec![("k", Value::Int(1)), ("req", text("x"))])],
            RowConflict::Reject,
        )]),
        "put",
    )?;
    ensure!(r.status == CommitStatus::Committed, "put: {r:?}");
    fx.advance(6000);
    let r = ok(
        fx.commit(vec![assert_op("strict", row_sel(kv_row(1), None), 1, 1)]),
        "assert",
    )?;
    ensure!(
        r.status == CommitStatus::Committed
            && r.ops[0].matched == 1
            && r.ops[0].affected == 0
            && r.ops[0].entries.is_empty(),
        "passing assert: {r:?}"
    );
    let row = fx
        .get_row("strict", 1)
        .ok_or_else(|| Fail::Failed("row gone".into()))?;
    ensure!(row.row_version == 1, "assert bumped the version: {row:?}");
    fx.advance(4000);
    ensure!(
        fx.get_row("strict", 1).is_none(),
        "assert refreshed AfterWrite expiry"
    );
    Ok(())
}

/// A failing `Assert` rejects the group and rolls back earlier operations;
/// a version guard fails with `VersionMismatch`.
fn assert_2(fx: &Fixture) -> T {
    ok(
        fx.put_kv(1, vec![("i", Value::Int(5))], RowConflict::Reject),
        "put",
    )?;
    let r = fx.commit(vec![
        put_rows(
            "kv",
            vec![row_of(vec![("k", Value::Int(2))])],
            RowConflict::Reject,
        ),
        assert_op(
            "kv",
            row_sel(
                kv_row(1),
                Some(Expr::cmp(CmpOp::Gt, Expr::field("i"), Expr::int(10))),
            ),
            1,
            1,
        ),
    ]);
    ensure!(
        rejected_for(&r, |x| *x == FailureReason::MatchExpectUnmet { matched: 0 }),
        "unmet assert: {r:?}"
    );
    ensure!(
        fx.get_row("kv", 2).is_none(),
        "an op before a failed assert survived"
    );
    let r = fx.commit(vec![WriteOp::Assert {
        target: o("kv"),
        selection: Selection::Keys {
            keys: vec![Keyed {
                key: KeyRef::Row(kv_row(1)),
                expected_version: Some(7),
            }],
            filter: None,
        },
        expect: MatchExpect { min: 0, max: 10 },
    }]);
    ensure!(
        rejected_for(&r, |x| *x
            == FailureReason::Entry(EntryStatus::VersionMismatch)),
        "stale version in assert: {r:?}"
    );
    let r = fx.commit(vec![assert_op("kv", row_sel(kv_row(1), None), 3, 1)]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "min above max: {r:?}"
    );
    Ok(())
}

/// `Assert` counts leased items whatever their lease, and does not change
/// `item_version`, so the worker's version-guarded finalize still commits.
fn assert_3(fx: &Fixture) -> T {
    let id = fx.push("as3", 1);
    let c = ok(fx.claim(1, 60_000), "claim")?;
    let item = c
        .items
        .first()
        .ok_or_else(|| Fail::Failed("nothing claimed".into()))?;
    let r = ok(
        fx.commit(vec![assert_op(
            "work",
            Selection::Where {
                predicate: Expr::eq(Expr::field("fw_state"), Expr::text("leased")),
            },
            1,
            1,
        )]),
        "assert leased",
    )?;
    ensure!(
        r.status == CommitStatus::Committed,
        "leased item not counted: {r:?}"
    );
    let v = fx
        .get_item(id, true)
        .ok_or_else(|| Fail::Failed("item missing".into()))?;
    ensure!(
        v.item_version == item.item_version,
        "assert changed item_version: {} -> {}",
        item.item_version,
        v.item_version
    );
    let mut e = FinalizeEntry::new(lease_of(item), FinalizeOutcome::Complete);
    e.expected_item_version = Some(item.item_version);
    let r = ok(fx.finalize(vec![e]), "finalize")?;
    ensure!(
        r.entries[0].status == EntryStatus::Completed,
        "guarded finalize after assert: {r:?}"
    );
    Ok(())
}

// ------------------------------------------------------------ merge rules

/// `NonDecreasing` on Int, Timestamp and Float: equal or larger values
/// apply; smaller or null values reject, through Set, Replace and Merge.
fn merge_1(fx: &Fixture) -> T {
    let put = |c: RowConflict, fs: Vec<(&str, Value)>| {
        let mut row = row_of(fs);
        row.insert(f("k"), text("a"));
        fx.commit(vec![put_rows("counters", vec![row], c)])
    };
    // A null old value allows anything.
    ensure!(
        committed(&put(RowConflict::Reject, vec![])),
        "insert without counters"
    );
    ensure!(
        committed(&put(
            RowConflict::Merge,
            vec![
                ("count", Value::Int(5)),
                ("hi", ts(100)),
                ("fl", Value::Float(1.5))
            ]
        )),
        "first values over null"
    );
    let set = |sets: Vec<(&str, Expr)>| {
        fx.commit(vec![WriteOp::Mutate(set_op("counters", ctr("a"), sets))])
    };
    let violated = |r: &DpResult<CommitResponse>, field: &str| {
        rejected_for(r, |x| {
            *x == FailureReason::Entry(EntryStatus::MergeRuleViolated { field: f(field) })
        })
    };
    let inc = Expr::arith(ArithOp::Add, Expr::field("count"), Expr::int(1));
    ensure!(committed(&set(vec![("count", inc)])), "increment");
    ensure!(
        committed(&set(vec![("count", Expr::field("count"))])),
        "equal value"
    );
    let dec = Expr::arith(ArithOp::Sub, Expr::field("count"), Expr::int(1));
    let r = set(vec![("count", dec)]);
    ensure!(violated(&r, "count"), "decrement: {r:?}");
    let r = set(vec![("count", Expr::null(ScalarType::Int))]);
    ensure!(violated(&r, "count"), "null over a value: {r:?}");
    let r = set(vec![("hi", lit(ts(99)))]);
    ensure!(violated(&r, "hi"), "earlier timestamp: {r:?}");
    let r = set(vec![("fl", lit(Value::Float(1.25)))]);
    ensure!(violated(&r, "fl"), "smaller float: {r:?}");
    let r = put(
        RowConflict::Replace,
        vec![
            ("count", Value::Int(3)),
            ("hi", ts(100)),
            ("fl", Value::Float(2.0)),
        ],
    );
    ensure!(violated(&r, "count"), "Replace lowering a counter: {r:?}");
    let r = put(RowConflict::Merge, vec![("hi", ts(50))]);
    ensure!(violated(&r, "hi"), "Merge lowering a timestamp: {r:?}");
    ensure!(
        committed(&put(RowConflict::Merge, vec![("count", Value::Int(9))])),
        "Merge raising a counter"
    );
    // A field without a rule is unconstrained.
    ensure!(
        committed(&set(vec![("free", Expr::int(-5))])),
        "unconstrained field"
    );
    let row = fx
        .get_row_key("counters", ctr("a"))
        .ok_or_else(|| Fail::Failed("row missing".into()))?;
    ensure!(
        row.fields[&f("count")] == Value::Int(9) && row.fields[&f("hi")] == ts(100),
        "final state: {row:?}"
    );
    Ok(())
}

/// `AddOnly` sets: unions apply; removing a member or clearing rejects.
fn merge_2(fx: &Fixture) -> T {
    let mut row = row_of(vec![("k", text("s"))]);
    row.insert(f("tags"), Value::TextSet(set_of(&["a"])));
    ok(
        fx.commit(vec![put_rows("counters", vec![row], RowConflict::Reject)]),
        "insert",
    )?;
    let set = |e: Expr| {
        fx.commit(vec![WriteOp::Mutate(set_op(
            "counters",
            ctr("s"),
            vec![("tags", e)],
        ))])
    };
    let add = Expr::SetAdd {
        set: bx(Expr::field("tags")),
        values: vec![Expr::text("b")],
    };
    ensure!(committed(&set(add)), "SetAdd");
    let remove = Expr::SetRemove {
        set: bx(Expr::field("tags")),
        values: vec![Expr::text("a")],
    };
    let violated = |r: &DpResult<CommitResponse>| {
        rejected_for(r, |x| {
            *x == FailureReason::Entry(EntryStatus::MergeRuleViolated { field: f("tags") })
        })
    };
    let r = set(remove);
    ensure!(violated(&r), "SetRemove: {r:?}");
    let r = set(Expr::null(ScalarType::TextSet));
    ensure!(violated(&r), "clear: {r:?}");
    let r = fx.commit(vec![put_rows(
        "counters",
        vec![row_of(vec![("k", text("s"))])],
        RowConflict::Replace,
    )]);
    ensure!(violated(&r), "Replace dropping the set: {r:?}");
    let mut sup = row_of(vec![("k", text("s"))]);
    sup.insert(f("tags"), Value::TextSet(set_of(&["a", "b", "c"])));
    ensure!(
        committed(&fx.commit(vec![put_rows("counters", vec![sup], RowConflict::Merge)])),
        "Merge with a superset"
    );
    let row = fx
        .get_row_key("counters", ctr("s"))
        .ok_or_else(|| Fail::Failed("row missing".into()))?;
    ensure!(
        row.fields[&f("tags")] == Value::TextSet(set_of(&["a", "b", "c"])),
        "final tags: {row:?}"
    );
    Ok(())
}

/// `override_merge_rules` (an explicit lift) lets a `Mutate` break the
/// rules; it is refused on queues.
fn merge_3(fx: &Fixture) -> T {
    let mut row = row_of(vec![("k", text("o")), ("count", Value::Int(4))]);
    row.insert(f("tags"), Value::TextSet(set_of(&["x"])));
    ok(
        fx.commit(vec![put_rows("counters", vec![row], RowConflict::Reject)]),
        "insert",
    )?;
    let mut lift = set_op(
        "counters",
        ctr("o"),
        vec![
            ("count", Expr::int(0)),
            ("tags", Expr::null(ScalarType::TextSet)),
        ],
    );
    lift.override_merge_rules = true;
    let r = fx.commit(vec![WriteOp::Mutate(lift)]);
    ensure!(committed(&r), "lift: {r:?}");
    let row = fx
        .get_row_key("counters", ctr("o"))
        .ok_or_else(|| Fail::Failed("row missing".into()))?;
    ensure!(
        row.fields[&f("count")] == Value::Int(0)
            && row.fields.get(&f("tags")).is_none_or(|v| v.is_null()),
        "after the lift: {row:?}"
    );
    let id = fx.push("mo", 1);
    let mut m = Mutate::new(
        "work",
        item_sel(id, None),
        Action::Set(vec![(f("n"), Expr::int(1))]),
        1,
    );
    m.override_merge_rules = true;
    let r = fx.commit(vec![WriteOp::Mutate(m)]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "override on a queue: {r:?}"
    );
    Ok(())
}

/// Merge rules apply only where they mean something.
fn merge_4(fx: &Fixture) -> T {
    use ScalarType::*;
    let try_table = |fields: Vec<FieldDecl>| {
        let mut s = base_schema();
        s.tables.push(TableDecl {
            name: o("bad"),
            key: vec![f("k")],
            fields,
            expiry: None,
            key_assignment: KeyAssignment::Caller,
        });
        fx.upgrade(s)
    };
    let key = || FieldDecl::stored("k", Int, false, IndexHint::None);
    for (what, fields) in [
        (
            "rule on the key",
            vec![key().with_merge(MergeRule::NonDecreasing)],
        ),
        (
            "AddOnly on Int",
            vec![
                key(),
                FieldDecl::stored("v", Int, true, IndexHint::None).with_merge(MergeRule::AddOnly),
            ],
        ),
        (
            "NonDecreasing on Text",
            vec![
                key(),
                FieldDecl::stored("v", Text, true, IndexHint::None)
                    .with_merge(MergeRule::NonDecreasing),
            ],
        ),
    ] {
        let r = try_table(fields);
        ensure!(
            matches!(r, Err(DataPlaneError::InvalidRequest(_))),
            "{what}: {r:?}"
        );
    }
    let mut s = base_schema();
    s.queues[0].fields.push(
        FieldDecl::stored("m", Int, true, IndexHint::None).with_merge(MergeRule::NonDecreasing),
    );
    let r = fx.upgrade(s);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "rule on a queue field: {r:?}"
    );
    Ok(())
}

// --------------------------------------------------------- sequence tables

fn tl_row(run: &str, what: &str) -> BTreeMap<FieldName, Value> {
    row_of(vec![("run", text(run)), ("what", text(what))])
}

fn assigned(r: &CommitResponse, op: usize) -> Vec<i64> {
    r.ops[op]
        .entries
        .iter()
        .filter_map(|e| match e.row_key.as_deref() {
            Some([Value::Int(n)]) => Some(*n),
            _ => None,
        })
        .collect()
}

/// Keys are assigned 1, 2, 3 ... across groups, returned in the entry, and
/// stored.
fn seq_1(fx: &Fixture) -> T {
    let r = ok(
        fx.commit(vec![put_rows(
            "timeline",
            vec![tl_row("r", "a"), tl_row("r", "b")],
            RowConflict::Reject,
        )]),
        "first",
    )?;
    ensure!(assigned(&r, 0) == vec![1, 2], "first group: {r:?}");
    let r = ok(
        fx.commit(vec![put_rows(
            "timeline",
            vec![tl_row("r", "c")],
            RowConflict::Reject,
        )]),
        "second",
    )?;
    ensure!(assigned(&r, 0) == vec![3], "second group: {r:?}");
    for (k, what) in [(1, "a"), (2, "b"), (3, "c")] {
        let row = fx
            .get_row("timeline", k)
            .ok_or_else(|| Fail::Failed(format!("row {k} missing")))?;
        ensure!(
            row.fields[&f("what")] == text(what) && row.fields[&f("seq")] == Value::Int(k),
            "row {k}: {row:?}"
        );
    }
    Ok(())
}

/// A rejected group assigns nothing; a supplied key is an invalid entry;
/// rows are immutable; a retired number is never reused.
fn seq_2(fx: &Fixture) -> T {
    ok(
        fx.commit(vec![put_rows(
            "timeline",
            vec![tl_row("r", "a")],
            RowConflict::Reject,
        )]),
        "first",
    )?;
    let r = fx.commit(vec![
        put_rows("timeline", vec![tl_row("r", "lost")], RowConflict::Reject),
        assert_op("kv", row_sel(kv_row(404), None), 1, 1),
    ]);
    ensure!(!committed(&r), "the group should reject: {r:?}");
    let mut keyed = tl_row("r", "keyed");
    keyed.insert(f("seq"), Value::Int(50));
    let r = fx.commit(vec![put_rows("timeline", vec![keyed], RowConflict::Reject)]);
    ensure!(
        rejected_for(&r, |x| matches!(
            x,
            FailureReason::Entry(EntryStatus::Invalid { .. })
        )),
        "a supplied key: {r:?}"
    );
    let r = ok(
        fx.commit(vec![put_rows(
            "timeline",
            vec![tl_row("r", "b")],
            RowConflict::Reject,
        )]),
        "after rejections",
    )?;
    ensure!(assigned(&r, 0) == vec![2], "gap after rejections: {r:?}");
    let r = fx.commit(vec![WriteOp::Mutate(set_op(
        "timeline",
        kv_row(1),
        vec![("what", Expr::text("x"))],
    ))]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "Set on a Sequence table: {r:?}"
    );
    let retire = Mutate::new("timeline", row_sel(kv_row(2), None), Action::Retire, 1);
    ensure!(
        committed(&fx.commit(vec![WriteOp::Mutate(retire)])),
        "retire"
    );
    let r = ok(
        fx.commit(vec![put_rows(
            "timeline",
            vec![tl_row("r", "c")],
            RowConflict::Reject,
        )]),
        "after retire",
    )?;
    ensure!(
        assigned(&r, 0) == vec![3],
        "a retired number was reused: {r:?}"
    );
    Ok(())
}

/// Readers page by key with a cursor that never goes backwards, and see
/// later appends after their cursor.
fn seq_3(fx: &Fixture) -> T {
    let rows = (0..7).map(|i| tl_row("r", &format!("e{i}"))).collect();
    ok(
        fx.commit(vec![put_rows("timeline", rows, RowConflict::Reject)]),
        "append",
    )?;
    let mut seen = vec![];
    let mut token = None;
    loop {
        let r = ok(
            fx.read_page(
                "timeline_after",
                params(vec![("after", Value::Int(0))]),
                token,
            ),
            "read",
        )?;
        ensure!(r.rows.len() <= 3, "page larger than max_rows");
        for row in &r.rows {
            if let Some(Value::Int(n)) = row.get(&f("seq")) {
                seen.push(*n);
            }
        }
        token = r.next_page_token;
        if token.is_none() {
            break;
        }
    }
    ensure!(seen == (1..=7).collect::<Vec<_>>(), "paged keys: {seen:?}");
    ok(
        fx.commit(vec![put_rows(
            "timeline",
            vec![tl_row("r", "late1"), tl_row("r", "late2")],
            RowConflict::Reject,
        )]),
        "append more",
    )?;
    let cursor = *seen.last().unwrap_or(&0);
    let r = ok(
        fx.read_page(
            "timeline_after",
            params(vec![("after", Value::Int(cursor))]),
            None,
        ),
        "read after cursor",
    )?;
    let next: Vec<_> = r
        .rows
        .iter()
        .filter_map(|row| match row.get(&f("seq")) {
            Some(Value::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    ensure!(next == vec![8, 9], "after the cursor: {next:?}");
    Ok(())
}

/// `Sequence` needs one non-null `Int` key field; key assignment is fixed.
fn seq_4(fx: &Fixture) -> T {
    use ScalarType::*;
    let bad = |key: Vec<&str>, fields: Vec<FieldDecl>| {
        let mut s = base_schema();
        s.tables.push(TableDecl {
            name: o("badseq"),
            key: key.into_iter().map(f).collect(),
            fields,
            expiry: None,
            key_assignment: KeyAssignment::Sequence,
        });
        fx.upgrade(s)
    };
    let r = bad(
        vec!["k"],
        vec![FieldDecl::stored("k", Text, false, IndexHint::None)],
    );
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "Text key: {r:?}"
    );
    let r = bad(
        vec!["a", "b"],
        vec![
            FieldDecl::stored("a", Int, false, IndexHint::None),
            FieldDecl::stored("b", Int, false, IndexHint::None),
        ],
    );
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "two-field key: {r:?}"
    );
    let mut s = base_schema();
    if let Some(t) = s.tables.iter_mut().find(|t| t.name.as_str() == "timeline") {
        t.key_assignment = KeyAssignment::Caller;
    }
    let r = fx.upgrade(s);
    ensure!(
        matches!(r, Err(DataPlaneError::SchemaConflict { .. })),
        "changing key assignment: {r:?}"
    );
    Ok(())
}

// ------------------------------------------------------- without fault hooks

/// Lost response: the caller never sees the response; `get_request` and a
/// replay return the committed result, and the effect applied once.
fn rec_5(fx: &Fixture) -> T {
    let h = fx.hdr();
    let ops = vec![WriteOp::Mutate(set_op(
        "counters",
        ctr("lost"),
        vec![("free", Expr::int(1))],
    ))];
    ok(
        fx.commit(vec![put_rows(
            "counters",
            vec![row_of(vec![("k", text("lost")), ("free", Value::Int(0))])],
            RowConflict::Reject,
        )]),
        "seed",
    )?;
    let first = ok(
        fx.commit_with(h.clone(), ops.clone(), Params::new(), false),
        "commit",
    )?;
    drop(first);
    let rec = ok(
        block_on(fx.plane.get_request(GetRequestRequest {
            space: sp(),
            request_id: h.request_id.clone(),
        })),
        "get_request",
    )?
    .ok_or_else(|| Fail::Failed("no request record".into()))?;
    let replay = ok(fx.commit_with(h, ops, Params::new(), false), "replay")?;
    ensure!(
        replay.status == CommitStatus::Committed,
        "replay of a committed request: {replay:?}"
    );
    ensure!(
        matches!(&rec.response, RecordedResponse::Commit(c) if c.status == CommitStatus::Committed),
        "recorded response: {:?}",
        rec.response
    );
    let row = fx
        .get_row_key("counters", ctr("lost"))
        .ok_or_else(|| Fail::Failed("row missing".into()))?;
    ensure!(row.row_version == 2, "applied more than once: {row:?}");
    Ok(())
}

/// Two instances on the same storage see each other's commits at once and
/// never lease one item twice.
fn multi_1(fx: &Fixture) -> T {
    let Some(peer) = fx.open_peer() else {
        return skip("harness gap: no second instance on the same storage");
    };
    fx.push("m1", 1);
    let r = ok(
        block_on(peer.get(GetRequest {
            space: sp(),
            targets: vec![GetTarget::Item {
                queue: o("work"),
                item: ItemRef::Key(ClientItemKey::new("m1")),
                include_terminal: false,
            }],
        })),
        "peer get",
    )?;
    ensure!(
        r.results.first().is_some_and(|x| x.is_some()),
        "the peer does not see a committed enqueue"
    );
    let items = (0..30)
        .map(|i| NewItem::new(Some(&format!("mm{i}")), PriorityValue::Int(i)))
        .collect();
    ok(fx.enqueue(items, KeyConflict::Duplicate), "enqueue")?;
    let planes: Vec<Arc<dyn DataPlane>> = vec![fx.plane.clone(), peer, fx.plane.clone()];
    let got: Vec<ItemId> = std::thread::scope(|scope| {
        let hs: Vec<_> = planes
            .iter()
            .enumerate()
            .map(|(w, p)| {
                scope.spawn(move || {
                    let mut ids = vec![];
                    for _ in 0..15 {
                        let req = ClaimRequest::new(
                            WriteHeader::new(sp(), fx.rid()),
                            "work",
                            &format!("w{w}"),
                            2,
                            60_000,
                        );
                        if let Ok(r) = block_on(p.claim(req)) {
                            ids.extend(r.items.into_iter().map(|i| i.item_id));
                        }
                    }
                    ids
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    let distinct: std::collections::BTreeSet<_> = got.iter().collect();
    ensure!(distinct.len() == got.len(), "an item was leased twice");
    ensure!(got.len() == 31, "claimed {} of 31", got.len());
    Ok(())
}

/// Requests on several threads (and instances, when the harness has
/// peers) conflict on one row while the clock moves. In commit order (the
/// row version each produced), `as_of` never decreases.
fn time_3(fx: &Fixture) -> T {
    ok(
        fx.commit(vec![put_rows(
            "counters",
            vec![row_of(vec![("k", text("t3")), ("free", Value::Int(0))])],
            RowConflict::Reject,
        )]),
        "seed",
    )?;
    let mut planes: Vec<Arc<dyn DataPlane>> = vec![fx.plane.clone(), fx.plane.clone()];
    if let Some(p) = fx.open_peer() {
        planes.push(p);
    }
    let mut seen: Vec<(u64, i64)> = std::thread::scope(|scope| {
        let ticker = scope.spawn(|| {
            for _ in 0..40 {
                fx.advance(1);
                std::thread::yield_now();
            }
        });
        let hs: Vec<_> = planes
            .iter()
            .map(|p| {
                scope.spawn(move || {
                    let mut out = vec![];
                    for _ in 0..10 {
                        let m = set_op(
                            "counters",
                            ctr("t3"),
                            vec![(
                                "free",
                                Expr::arith(ArithOp::Add, Expr::field("free"), Expr::int(1)),
                            )],
                        );
                        let r = block_on(p.commit(CommitRequest {
                            header: WriteHeader::new(sp(), fx.rid()),
                            ops: vec![WriteOp::Mutate(m)],
                            params: Params::new(),
                            dry_run: false,
                        }));
                        if let Ok(r) = r
                            && r.status == CommitStatus::Committed
                            && let Some(v) = r.ops[0].entries.first().and_then(|e| e.version)
                        {
                            out.push((v, r.as_of.0));
                        }
                    }
                    out
                })
            })
            .collect();
        ticker.join().unwrap();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    seen.sort();
    ensure!(
        seen.len() == planes.len() * 10,
        "only {} of {} requests committed",
        seen.len(),
        planes.len() * 10
    );
    for w in seen.windows(2) {
        ensure!(
            w[0].0 < w[1].0 && w[0].1 <= w[1].1,
            "as_of decreased along commit order: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
    Ok(())
}

/// Concurrent group commits either commit whole or reject whole (or report
/// `Contention` after the implementation's retries); no partial effect.
fn pg_1(fx: &Fixture) -> T {
    let mut planes: Vec<Arc<dyn DataPlane>> = vec![fx.plane.clone(), fx.plane.clone()];
    if let Some(p) = fx.open_peer() {
        planes.push(p);
    }
    ok(fx.put_kv(7, vec![], RowConflict::Reject), "guard row")?;
    let outcomes: Vec<(i64, bool)> = std::thread::scope(|scope| {
        let hs: Vec<_> = planes
            .iter()
            .enumerate()
            .map(|(w, p)| {
                scope.spawn(move || {
                    let mut out = vec![];
                    for i in 0..8 {
                        let base = 1000 + (w as i64) * 100 + i * 2;
                        // Two rows, then a guard that fails for odd i.
                        let guard_key = if i % 2 == 0 { 7 } else { 404 };
                        let r = block_on(p.commit(CommitRequest {
                            header: WriteHeader::new(sp(), fx.rid()),
                            ops: vec![
                                put_rows(
                                    "kv",
                                    vec![row_of(vec![("k", Value::Int(base))])],
                                    RowConflict::Reject,
                                ),
                                put_rows(
                                    "kv",
                                    vec![row_of(vec![("k", Value::Int(base + 1))])],
                                    RowConflict::Reject,
                                ),
                                assert_op("kv", row_sel(kv_row(guard_key), None), 1, 1),
                            ],
                            params: Params::new(),
                            dry_run: false,
                        }));
                        let ok = matches!(&r, Ok(x) if x.status == CommitStatus::Committed);
                        let allowed = ok
                            || matches!(&r, Ok(x) if x.status == CommitStatus::Rejected)
                            || matches!(&r, Err(DataPlaneError::Contention));
                        assert!(allowed, "unexpected outcome {r:?}");
                        out.push((base, ok));
                    }
                    out
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    for (base, committed) in outcomes {
        let a = fx.get_row("kv", base).is_some();
        let b = fx.get_row("kv", base + 1).is_some();
        ensure!(
            a == committed && b == committed,
            "group at {base}: committed {committed}, rows {a} {b}"
        );
    }
    Ok(())
}

/// Write skew: two groups each require the other's key absent and insert
/// their own. At most one commits.
fn pg_3(fx: &Fixture) -> T {
    let mut planes: Vec<Arc<dyn DataPlane>> = vec![fx.plane.clone()];
    planes.push(fx.open_peer().unwrap_or_else(|| fx.plane.clone()));
    for round in 0..10i64 {
        let (a, b) = (2000 + round * 2, 2001 + round * 2);
        let wins: usize = std::thread::scope(|scope| {
            let hs: Vec<_> = [(a, b), (b, a)]
                .into_iter()
                .zip(planes.iter())
                .map(|((mine, other), p)| {
                    scope.spawn(move || {
                        let r = block_on(p.commit(CommitRequest {
                            header: WriteHeader::new(sp(), fx.rid()),
                            ops: vec![
                                assert_op("kv", row_sel(kv_row(other), None), 0, 0),
                                put_rows(
                                    "kv",
                                    vec![row_of(vec![("k", Value::Int(mine))])],
                                    RowConflict::Reject,
                                ),
                            ],
                            params: Params::new(),
                            dry_run: false,
                        }));
                        usize::from(matches!(r, Ok(x) if x.status == CommitStatus::Committed))
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).sum()
        });
        let present = [a, b]
            .iter()
            .filter(|k| fx.get_row("kv", **k).is_some())
            .count();
        ensure!(
            wins <= 1 && present == wins,
            "round {round}: {wins} commits, {present} rows"
        );
    }
    Ok(())
}
