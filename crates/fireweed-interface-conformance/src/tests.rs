//! The rule-pinning tests, by API-006 id.

use std::collections::BTreeMap;

use fireweed_interface::*;
use futures::executor::block_on;

use crate::ensure;
use crate::support::*;

type Test = fn(&Fixture) -> T;

pub const ALL: &[(&str, Test)] = &[
    ("TYP-1", typ_1),
    ("TYP-2", typ_2),
    ("EVAL-1", eval_1),
    ("EVAL-2", eval_2),
    ("EVAL-3", eval_3),
    ("EVAL-4", eval_4),
    ("EVAL-5", eval_5),
    ("EVAL-6", eval_6),
    ("EVAL-7", eval_7),
    ("EVAL-8", eval_8),
    ("EVAL-9", eval_9),
    ("EVAL-10", eval_10),
    ("EVAL-11", eval_11),
    ("CAST-1", cast_1),
    ("CAST-2", cast_2),
    ("JSON-1", json_1),
    ("JSON-2", json_2),
    ("JSON-3", json_3),
    ("AGG-1", agg_1),
    ("AGG-2", agg_2),
    ("AGG-3", agg_3),
    ("AGG-4", agg_4),
    ("IDX-1", idx_1),
    ("IDX-2", |_| {
        skip("needs an index that is still building; no harness control yet")
    }),
    ("SCAN-1", scan_1),
    ("SCAN-2", scan_2),
    ("SCAN-3", scan_3),
    ("SCAN-4", scan_4),
    ("SCAN-5", scan_5),
    ("SCAN-6", |_| {
        skip("implementation-specific: needs expired rows that are still stored")
    }),
    ("LIM-1", lim_1),
    ("LIM-2", lim_2),
    ("GRP-1", grp_1),
    ("GRP-2", grp_2),
    ("GRP-3", grp_3),
    ("GRP-4", grp_4),
    ("GRP-5", grp_5),
    ("GRP-6", grp_6),
    ("GRP-7", grp_7),
    ("GRP-8", grp_8),
    ("GRP-9", grp_9),
    ("GRP-10", grp_10),
    ("GRP-11", grp_11),
    ("GRP-12", grp_12),
    ("GRP-13", grp_13),
    ("GRP-14", grp_14),
    ("PATCH-1", patch_1),
    ("FIN-1", fin_1),
    ("CHK-1", chk_1),
    ("CHK-2", chk_2),
    ("DRY-1", dry_1),
    ("REC-1", rec_1),
    ("REC-2", rec_2),
    ("REC-3", rec_3),
    ("REC-4", |_| {
        skip("needs the StallAtReceive fault hook and a child process")
    }),
    ("REC-5", |_| {
        skip("needs the AfterCommitBeforeResponse fault hook")
    }),
    ("REC-6", rec_6),
    ("REC-7", rec_7),
    ("REC-8", rec_8),
    ("TIME-1", time_1),
    ("TIME-3", |_| {
        skip("needs concurrent peers in child processes")
    }),
    ("TIME-4", time_4),
    ("TIME-5", time_5),
    ("COH-1", coh_1),
    ("DUR-1", dur_1),
    ("DUR-2", dur_2),
    ("DUR-3", |f| {
        if_replicated(f, "needs the HoldReplication fault hook")
    }),
    ("DUR-4", |_| skip("needs a child process and SIGKILL")),
    ("DUR-5", |f| if_replicated(f, "needs lose_local_storage")),
    ("DUR-6", |f| {
        if_replicated(f, "needs the HoldReplication fault hook")
    }),
    ("LIN-1", |_| {
        skip("needs concurrent peers in child processes")
    }),
    ("LIN-2", lin_2),
    ("SHARE-1", |_| {
        skip("needs a second instance on the same storage")
    }),
    ("LOG-1", |f| needs_shape(f, ShapeKind::EventLog)),
    ("LOG-2", |f| needs_shape(f, ShapeKind::EventLog)),
    ("LOG-3", |f| needs_shape(f, ShapeKind::EventLog)),
    ("HIST-1", |f| {
        if f.caps().queue_history {
            skip("history test not written yet")
        } else {
            skip("queue history not declared")
        }
    }),
    ("BLOB-1", |f| needs_shape(f, ShapeKind::Blob)),
    ("BLOB-2", |f| needs_shape(f, ShapeKind::Blob)),
    ("BLOB-3", |f| needs_shape(f, ShapeKind::Blob)),
    ("SCH-1", sch_1),
    ("READ-1", read_1),
    ("READ-2", read_2),
    ("ENQ-1", enq_1),
    ("TBL-1", tbl_1),
    ("MET-1", met_1),
    ("PG-1", |_| skip("PostgreSQL implementation only")),
    ("PG-2", |_| skip("PostgreSQL implementation only")),
    ("PG-3", |_| skip("PostgreSQL implementation only")),
];

fn needs_shape(fx: &Fixture, s: ShapeKind) -> T {
    if fx.caps().shapes.contains(&s) {
        Err(Fail::Failed(format!(
            "{s:?} is declared but its tests are not written yet"
        )))
    } else {
        skip(&format!("{s:?} shape not declared"))
    }
}

fn if_replicated(fx: &Fixture, why: &str) -> T {
    if fx.caps().max_durability >= DurabilityLevel::Replicated {
        Err(Fail::Failed(format!("Replicated is declared; {why}")))
    } else {
        skip("Replicated not declared")
    }
}

// ----------------------------------------------------------- expressions

fn lit(v: Value) -> Expr {
    Expr::Lit(v)
}
fn bx(e: Expr) -> Box<Expr> {
    Box::new(e)
}
fn div(a: Expr, b: Expr) -> Expr {
    Expr::arith(ArithOp::Div, a, b)
}
fn act(fx: &Fixture, e: Expr, t: ScalarType) -> Result<Value, Fail> {
    match eval_action(fx, e.clone(), t)? {
        Ok(v) => Ok(v),
        Err(r) => Err(Fail::Failed(format!("{e:?} rejected: {r:?}"))),
    }
}
fn act_fault(fx: &Fixture, e: Expr, t: ScalarType) -> Result<FailureReason, Fail> {
    match eval_action(fx, e.clone(), t)? {
        Ok(v) => Err(Fail::Failed(format!("{e:?} gave {v:?}, expected a fault"))),
        Err(r) => Ok(r),
    }
}
fn invalid_set(fx: &Fixture, field: &str, e: Expr) -> T {
    fx.put_kv(1, vec![], RowConflict::Skip).ok();
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f(field), e.clone())]),
        1,
    );
    match fx.commit(vec![WriteOp::Mutate(m)]) {
        Err(DataPlaneError::InvalidRequest(_)) => Ok(()),
        other => Err(Fail::Failed(format!(
            "{e:?}: expected InvalidRequest, got {other:?}"
        ))),
    }
}

fn typ_1(fx: &Fixture) -> T {
    use ScalarType::*;
    invalid_set(fx, "b", Expr::eq(Expr::text("a"), Expr::int(1)))?;
    invalid_set(
        fx,
        "b",
        Expr::cmp(CmpOp::Lt, Expr::boolean(true), Expr::boolean(false)),
    )?;
    invalid_set(
        fx,
        "i",
        Expr::arith(ArithOp::Add, Expr::int(1), Expr::float(1.0)),
    )?;
    invalid_set(
        fx,
        "fl",
        Expr::arith(ArithOp::Rem, Expr::float(1.0), Expr::float(1.0)),
    )?;
    invalid_set(
        fx,
        "b",
        Expr::Contains {
            set: bx(Expr::text("a")),
            value: bx(Expr::text("a")),
        },
    )?;
    invalid_set(fx, "i", Expr::Coalesce(vec![Expr::int(1), Expr::text("x")]))?;
    invalid_set(
        fx,
        "b",
        Expr::eq(
            lit(Value::Json(serde_json::json!(1))),
            lit(Value::Json(serde_json::json!(1))),
        ),
    )?;
    invalid_set(fx, "i", Expr::text("not an int"))?;
    invalid_set(
        fx,
        "i",
        Expr::cast(Expr::lit(Value::TextSet(set_of(&["a"]))), Int),
    )?;
    ensure!(
        act(
            fx,
            Expr::arith(ArithOp::Add, Expr::int(1), Expr::int(2)),
            Int
        )? == Value::Int(3),
        "1+2"
    );
    Ok(())
}

fn typ_2(fx: &Fixture) -> T {
    use ScalarType::*;
    let v = act(fx, Expr::eq(Expr::null(Int), Expr::int(1)), Bool)?;
    ensure!(v == Value::Null(Bool), "typed null comparison gave {v:?}");
    fx.put_kv(1, vec![], RowConflict::Skip).ok();
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f("i"), Expr::param("p"))]),
        1,
    );
    let params: Params = [(ParamName::new("p"), text("x"))].into_iter().collect();
    let r = fx.commit_with(fx.hdr(), vec![WriteOp::Mutate(m)], params, false);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "mistyped parameter gave {r:?}"
    );
    Ok(())
}

fn zero_div() -> Expr {
    Expr::eq(div(Expr::int(1), Expr::int(0)), Expr::int(0))
}

fn eval_1(fx: &Fixture) -> T {
    use ScalarType::*;
    let v = act(
        fx,
        Expr::Coalesce(vec![Expr::int(1), div(Expr::int(1), Expr::int(0))]),
        Int,
    )?;
    ensure!(v == Value::Int(1), "Coalesce short-circuit gave {v:?}");
    let v = act(fx, Expr::And(vec![Expr::boolean(false), zero_div()]), Bool)?;
    ensure!(v == Value::Bool(false), "And short-circuit gave {v:?}");
    let v = act(fx, Expr::Or(vec![Expr::boolean(true), zero_div()]), Bool)?;
    ensure!(v == Value::Bool(true), "Or short-circuit gave {v:?}");
    Ok(())
}

fn eval_2(fx: &Fixture) -> T {
    ensure!(
        !eval_pred(fx, zero_div())?,
        "faulting predicate selected the row"
    );
    ensure!(
        eval_pred(fx, Expr::Or(vec![zero_div(), Expr::boolean(true)]))?,
        "fault should be null, so Or(null, true) is true"
    );
    ensure!(
        !eval_pred(fx, Expr::Not(bx(zero_div())))?,
        "Not(null) must not select"
    );
    Ok(())
}

fn eval_3(fx: &Fixture) -> T {
    fx.put_kv(1, vec![("i", Value::Int(5))], RowConflict::Replace)
        .map_err(|e| Fail::Failed(format!("{e:?}")))?;
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(
            f("i"),
            Expr::arith(ArithOp::Add, Expr::int(i64::MAX), Expr::int(1)),
        )]),
        1,
    );
    let h = fx.hdr();
    let r = ok(
        fx.commit_with(
            h.clone(),
            vec![WriteOp::Mutate(m.clone())],
            Params::new(),
            false,
        ),
        "commit",
    )?;
    ensure!(
        r.status == CommitStatus::Rejected,
        "overflow did not reject: {r:?}"
    );
    ensure!(
        matches!(
            r.failure.as_ref().map(|x| &x.reason),
            Some(FailureReason::Evaluation(EvalError::IntOverflow))
        ),
        "reason {:?}",
        r.failure
    );
    ensure!(
        fx.get_row("kv", 1).unwrap().fields[&f("i")] == Value::Int(5),
        "row changed"
    );
    let again = ok(
        fx.commit_with(h, vec![WriteOp::Mutate(m)], Params::new(), false),
        "replay",
    )?;
    ensure!(again == r, "replay differs from the recorded rejection");
    Ok(())
}

fn eval_4(fx: &Fixture) -> T {
    use ScalarType::*;
    let v = act(fx, div(Expr::int(-7), Expr::int(2)), Int)?;
    ensure!(v == Value::Int(-3), "-7/2 gave {v:?}");
    let v = act(
        fx,
        Expr::arith(ArithOp::Rem, Expr::int(-7), Expr::int(2)),
        Int,
    )?;
    ensure!(v == Value::Int(-1), "-7%2 gave {v:?}");
    let r = act_fault(fx, div(Expr::int(i64::MIN), Expr::int(-1)), Int)?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::IntOverflow),
        "MIN/-1 gave {r:?}"
    );
    let r = act_fault(fx, div(Expr::int(1), Expr::int(0)), Int)?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::DivideByZero),
        "1/0 gave {r:?}"
    );
    Ok(())
}

fn eval_5(fx: &Fixture) -> T {
    use ScalarType::*;
    let r = act_fault(fx, div(Expr::float(1.0), Expr::float(0.0)), Float)?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::FloatOutOfRange),
        "1.0/0.0 gave {r:?}"
    );
    let r = act_fault(
        fx,
        Expr::arith(ArithOp::Mul, Expr::float(1e200), Expr::float(1e200)),
        Float,
    )?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::FloatOutOfRange),
        "overflow gave {r:?}"
    );
    Ok(())
}

fn eval_6(fx: &Fixture) -> T {
    use ScalarType::*;
    let n = || Expr::null(Bool);
    let cases = vec![
        (Expr::And(vec![Expr::boolean(true), n()]), Value::Null(Bool)),
        (
            Expr::And(vec![Expr::boolean(false), n()]),
            Value::Bool(false),
        ),
        (
            Expr::And(vec![n(), Expr::boolean(false)]),
            Value::Bool(false),
        ),
        (Expr::Or(vec![Expr::boolean(true), n()]), Value::Bool(true)),
        (Expr::Or(vec![n(), Expr::boolean(true)]), Value::Bool(true)),
        (Expr::Or(vec![Expr::boolean(false), n()]), Value::Null(Bool)),
        (Expr::Not(bx(n())), Value::Null(Bool)),
        (
            Expr::In {
                expr: bx(Expr::int(1)),
                list: vec![Expr::int(2), Expr::null(Int)],
            },
            Value::Null(Bool),
        ),
        (
            Expr::In {
                expr: bx(Expr::int(1)),
                list: vec![Expr::int(1), Expr::null(Int)],
            },
            Value::Bool(true),
        ),
        (
            Expr::In {
                expr: bx(Expr::null(Int)),
                list: vec![Expr::int(1)],
            },
            Value::Null(Bool),
        ),
        (
            Expr::In {
                expr: bx(Expr::int(3)),
                list: vec![Expr::int(1), Expr::int(2)],
            },
            Value::Bool(false),
        ),
        (Expr::IsNull(bx(Expr::null(Int))), Value::Bool(true)),
    ];
    for (e, want) in cases {
        let v = act(fx, e.clone(), Bool)?;
        ensure!(v == want, "{e:?}: got {v:?}, want {want:?}");
    }
    Ok(())
}

fn eval_7(fx: &Fixture) -> T {
    use ScalarType::*;
    let lt = |a: Expr, b: Expr| Expr::cmp(CmpOp::Lt, a, b);
    ensure!(
        act(fx, lt(Expr::text("Z"), Expr::text("a")), Bool)? == Value::Bool(true),
        "Z < a"
    );
    ensure!(
        act(fx, lt(Expr::text("f"), Expr::text("é")), Bool)? == Value::Bool(true),
        "f < é by code point"
    );
    ensure!(
        act(fx, lt(Expr::text("é"), Expr::text("😀")), Bool)? == Value::Bool(true),
        "é < emoji"
    );
    let b = |x: &[u8]| lit(Value::Bytes(bytes::Bytes::copy_from_slice(x)));
    ensure!(
        act(fx, lt(b(&[1]), b(&[1, 0])), Bool)? == Value::Bool(true),
        "prefix first"
    );
    ensure!(
        act(fx, lt(b(&[1]), b(&[255])), Bool)? == Value::Bool(true),
        "unsigned bytes"
    );
    Ok(())
}

fn eval_8(fx: &Fixture) -> T {
    use ScalarType::*;
    let ts = |ms: i64| lit(Value::Timestamp(UtcMillis(ms)));
    let v = act(
        fx,
        Expr::Bucket {
            expr: bx(ts(-1)),
            width_ms: 1000,
        },
        Timestamp,
    )?;
    ensure!(
        v == Value::Timestamp(UtcMillis(-1000)),
        "bucket(-1, 1000) gave {v:?}"
    );
    let v = act(
        fx,
        Expr::Bucket {
            expr: bx(ts(1999)),
            width_ms: 1000,
        },
        Timestamp,
    )?;
    ensure!(
        v == Value::Timestamp(UtcMillis(1000)),
        "bucket(1999, 1000) gave {v:?}"
    );
    invalid_set(
        fx,
        "ts",
        Expr::Bucket {
            expr: bx(ts(0)),
            width_ms: 0,
        },
    )?;
    invalid_set(
        fx,
        "ts",
        Expr::Bucket {
            expr: bx(ts(0)),
            width_ms: 315_576_000_001,
        },
    )?;
    Ok(())
}

fn eval_9(fx: &Fixture) -> T {
    fx.put_kv(1, vec![], RowConflict::Skip).ok();
    fx.put_kv(2, vec![], RowConflict::Skip).ok();
    let set_now = |k| {
        WriteOp::Mutate(Mutate::new(
            "kv",
            keys(&[k]),
            Action::Set(vec![(f("ts"), Expr::Now)]),
            1,
        ))
    };
    let r = ok(fx.commit(vec![set_now(1), set_now(2)]), "commit")?;
    let a = fx.get_row("kv", 1).unwrap().fields[&f("ts")].clone();
    let b = fx.get_row("kv", 2).unwrap().fields[&f("ts")].clone();
    ensure!(a == b, "Now differs within one request: {a:?} {b:?}");
    ensure!(
        a == Value::Timestamp(r.as_of),
        "Now {a:?} is not the request time {:?}",
        r.as_of
    );
    Ok(())
}

fn eval_10(fx: &Fixture) -> T {
    let max = fx.limits().max_set_values as usize;
    let values: Vec<Expr> = (0..=max).map(|i| Expr::text(&format!("v{i}"))).collect();
    let r = act_fault(
        fx,
        Expr::SetAdd {
            set: bx(Expr::null(ScalarType::TextSet)),
            values,
        },
        ScalarType::TextSet,
    )?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::SetTooLarge),
        "oversized set gave {r:?}"
    );
    let v = act(
        fx,
        Expr::SetRemove {
            set: bx(lit(Value::TextSet(set_of(&["a"])))),
            values: vec![Expr::text("zz")],
        },
        ScalarType::TextSet,
    )?;
    ensure!(
        v == Value::TextSet(set_of(&["a"])),
        "SetRemove of an absent value gave {v:?}"
    );
    let v = act(
        fx,
        Expr::SetAdd {
            set: bx(lit(Value::TextSet(set_of(&["a"])))),
            values: vec![Expr::null(ScalarType::Text)],
        },
        ScalarType::TextSet,
    )?;
    ensure!(
        v == Value::TextSet(set_of(&["a"])),
        "SetAdd of null gave {v:?}"
    );
    Ok(())
}

fn eval_11(fx: &Fixture) -> T {
    use ScalarType::*;
    fx.put_kv(1, vec![], RowConflict::Skip).ok();
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f("s"), Expr::param("p"))]),
        1,
    );
    let params: Params = [(ParamName::new("p"), text("a\0b"))].into_iter().collect();
    let r = fx.commit_with(fx.hdr(), vec![WriteOp::Mutate(m)], params, false);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "U+0000 parameter gave {r:?}"
    );
    invalid_set(fx, "fl", Expr::float(1e291))?;
    let r = act_fault(
        fx,
        Expr::arith(ArithOp::Mul, Expr::float(1e200), Expr::float(1e100)),
        Float,
    )?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::FloatOutOfRange),
        "1e300 gave {r:?}"
    );
    let r = act_fault(
        fx,
        Expr::arith(
            ArithOp::Add,
            lit(Value::Timestamp(UtcMillis(TIMESTAMP_MAX))),
            Expr::int(1),
        ),
        Timestamp,
    )?;
    ensure!(
        r == FailureReason::Evaluation(EvalError::TimestampOutOfRange),
        "year 10000 gave {r:?}"
    );
    Ok(())
}

fn cast_1(fx: &Fixture) -> T {
    use ScalarType::*;
    let j = |v: serde_json::Value| lit(Value::Json(v));
    let cases: Vec<(Expr, ScalarType, Value)> = vec![
        (Expr::int(3), Float, Value::Float(3.0)),
        (Expr::float(-2.7), Int, Value::Int(-2)),
        (Expr::float(1e20), Int, Value::Null(Int)),
        (Expr::text("+1"), Int, Value::Null(Int)),
        (Expr::text(" 1"), Int, Value::Null(Int)),
        (Expr::text("1_000"), Int, Value::Null(Int)),
        (Expr::text("12abc"), Int, Value::Null(Int)),
        (Expr::text("-0"), Int, Value::Int(0)),
        (Expr::text("1e3"), Float, Value::Float(1000.0)),
        (Expr::text("inf"), Float, Value::Null(Float)),
        (Expr::text("NaN"), Float, Value::Null(Float)),
        (Expr::text("true"), Bool, Value::Bool(true)),
        (Expr::text("TRUE"), Bool, Value::Null(Bool)),
        (Expr::boolean(false), Text, text("false")),
        (Expr::int(-42), Text, text("-42")),
        (
            lit(Value::Timestamp(UtcMillis(0))),
            Text,
            text("1970-01-01T00:00:00.000Z"),
        ),
        (
            lit(Value::Timestamp(UtcMillis(1234))),
            Int,
            Value::Int(1234),
        ),
        (
            Expr::int(TIMESTAMP_MAX + 1),
            Timestamp,
            Value::Null(Timestamp),
        ),
        (j(serde_json::json!(3)), Int, Value::Int(3)),
        (j(serde_json::json!(3.5)), Int, Value::Null(Int)),
        (j(serde_json::json!(3.5)), Float, Value::Float(3.5)),
        (j(serde_json::json!("x")), Text, text("x")),
        (j(serde_json::json!(1)), Text, Value::Null(Text)),
        (j(serde_json::json!(true)), Bool, Value::Bool(true)),
        (j(serde_json::json!("true")), Bool, Value::Null(Bool)),
    ];
    for (e, to, want) in cases {
        let v = act(fx, Expr::cast(e.clone(), to), to)?;
        ensure!(v == want, "cast {e:?} to {to:?}: got {v:?}, want {want:?}");
    }
    invalid_set(fx, "i", Expr::cast(Expr::boolean(true), Int))?;
    Ok(())
}

fn cast_2(fx: &Fixture) -> T {
    use ScalarType::*;
    let cases = [
        ("1970-01-01T01:00:00.9999+01:00", Some(999)),
        ("1970-01-01t00:00:01z", Some(1000)),
        ("2016-12-31T23:59:60Z", None),
        ("2024-02-30T00:00:00Z", None),
        ("1970-01-01T00:00:00", None),
    ];
    for (s, want) in cases {
        let v = act(fx, Expr::cast(Expr::text(s), Timestamp), Timestamp)?;
        let want = want
            .map(|m| Value::Timestamp(UtcMillis(m)))
            .unwrap_or(Value::Null(Timestamp));
        ensure!(v == want, "parse {s:?}: got {v:?}, want {want:?}");
    }
    Ok(())
}

fn json_1(fx: &Fixture) -> T {
    let v = act(
        fx,
        lit(Value::Json(serde_json::json!({"a": 3.0, "b": [1.0, 2.5]}))),
        ScalarType::Json,
    )?;
    ensure!(
        v == Value::Json(serde_json::json!({"a": 3, "b": [1, 2.5]})),
        "normalization gave {v:?}"
    );
    let big: serde_json::Value = serde_json::from_str("18446744073709551615").unwrap();
    invalid_set(fx, "j", lit(Value::Json(big)))?;
    Ok(())
}

fn json_2(fx: &Fixture) -> T {
    use ScalarType::*;
    let cases: Vec<(Expr, serde_json::Value)> = vec![
        (
            lit(Value::Bytes(bytes::Bytes::from_static(&[1, 2]))),
            serde_json::json!("AQI="),
        ),
        (
            lit(Value::Timestamp(UtcMillis(1))),
            serde_json::json!("1970-01-01T00:00:00.001Z"),
        ),
        (
            lit(Value::TextSet(set_of(&["b", "a"]))),
            serde_json::json!(["a", "b"]),
        ),
        (Expr::int(7), serde_json::json!(7)),
        (Expr::text("x"), serde_json::json!("x")),
    ];
    for (e, want) in cases {
        let v = act(fx, Expr::cast(e.clone(), Json), Json)?;
        ensure!(
            v == Value::Json(want.clone()),
            "{e:?} as JSON: got {v:?}, want {want}"
        );
    }
    Ok(())
}

fn json_3(fx: &Fixture) -> T {
    use ScalarType::*;
    ok(
        fx.put_kv(
            1,
            vec![("j", Value::Json(serde_json::json!({"a": null})))],
            RowConflict::Replace,
        ),
        "put",
    )?;
    let ptr = |p: &str| Expr::Json {
        field: f("j"),
        pointer: JsonPointer::new(p),
    };
    let v = act(fx, ptr("/a"), Json)?;
    ensure!(
        v == Value::Json(serde_json::Value::Null),
        "JSON null gave {v:?}"
    );
    ok(
        fx.put_kv(
            1,
            vec![("j", Value::Json(serde_json::json!({"a": null})))],
            RowConflict::Replace,
        ),
        "put",
    )?;
    let v = act(fx, ptr("/b"), Json)?;
    ensure!(v == Value::Null(Json), "absent pointer gave {v:?}");
    ok(
        fx.put_kv(
            1,
            vec![("j", Value::Json(serde_json::json!({"a": null})))],
            RowConflict::Replace,
        ),
        "put",
    )?;
    let v = act(fx, Expr::cast(ptr("/a"), Int), Int)?;
    ensure!(v == Value::Null(Int), "cast of JSON null gave {v:?}");
    Ok(())
}

// ------------------------------------------------------------ aggregates

fn put_nums(fx: &Fixture, rows: Vec<(i64, &str, Option<i64>, Option<f64>)>) -> T {
    let rows = rows
        .into_iter()
        .map(|(id, g, v, x)| {
            [
                (f("id"), Value::Int(id)),
                (f("g"), text(g)),
                (
                    f("v"),
                    v.map(Value::Int).unwrap_or(Value::Null(ScalarType::Int)),
                ),
                (
                    f("x"),
                    x.map(Value::Float)
                        .unwrap_or(Value::Null(ScalarType::Float)),
                ),
            ]
            .into_iter()
            .collect()
        })
        .collect();
    let r = ok(
        fx.commit(vec![WriteOp::PutRows {
            table: o("nums"),
            rows,
            on_conflict: RowConflict::Reject,
        }]),
        "put nums",
    )?;
    ensure!(
        r.status == CommitStatus::Committed,
        "put nums rejected: {:?}",
        r.failure
    );
    Ok(())
}

fn one_row(r: &ReadResponse) -> Result<&BTreeMap<FieldName, Value>, Fail> {
    r.rows.first().ok_or_else(|| Fail::Failed("no row".into()))
}

fn agg_1(fx: &Fixture) -> T {
    use ScalarType::*;
    put_nums(
        fx,
        vec![
            (1, "a", Some(1), Some(0.5)),
            (2, "a", Some(2), Some(1.5)),
            (3, "a", None, None),
        ],
    )?;
    let r = ok(fx.read("stats", vec![("g", text("a"))]), "read")?;
    let row = one_row(&r)?;
    let want = [
        ("count", Value::Int(3)),
        ("count_v", Value::Int(2)),
        ("sum_v", Value::Int(3)),
        ("min_v", Value::Int(1)),
        ("max_v", Value::Int(2)),
        ("avg_v", Value::Float(1.5)),
        ("sum_x", Value::Float(2.0)),
        ("avg_x", Value::Float(1.0)),
    ];
    for (k, v) in want {
        ensure!(
            row.get(&f(k)) == Some(&v),
            "{k}: got {:?}, want {v:?}",
            row.get(&f(k))
        );
    }
    let r = ok(fx.read("stats", vec![("g", text("none"))]), "read empty")?;
    ensure!(
        r.rows.len() == 1,
        "empty group_by must give one row, got {}",
        r.rows.len()
    );
    let row = one_row(&r)?;
    ensure!(
        row[&f("count")] == Value::Int(0) && row[&f("count_v")] == Value::Int(0),
        "empty counts"
    );
    ensure!(
        row[&f("sum_v")] == Value::Null(Int) && row[&f("avg_x")] == Value::Null(Float),
        "empty sums not null"
    );
    let r = ok(fx.read("grouped", vec![("g", text("a"))]), "grouped")?;
    ensure!(
        r.rows.len() == 3,
        "groups (null, 1, 2) expected, got {}",
        r.rows.len()
    );
    let r = ok(
        fx.read("grouped", vec![("g", text("none"))]),
        "grouped empty",
    )?;
    ensure!(
        r.rows.is_empty(),
        "non-empty group_by over no rows must give no rows"
    );
    Ok(())
}

fn agg_2(fx: &Fixture) -> T {
    put_nums(
        fx,
        vec![
            (1, "b", Some(i64::MAX), None),
            (2, "b", Some(1), None),
            (3, "b", Some(-1), None),
        ],
    )?;
    let r = ok(fx.read("stats", vec![("g", text("b"))]), "read")?;
    let row = one_row(&r)?;
    ensure!(
        row[&f("sum_v")] == Value::Int(i64::MAX),
        "running overflow, final fits: got {:?}",
        row[&f("sum_v")]
    );
    put_nums(
        fx,
        vec![(4, "c", Some(i64::MAX), None), (5, "c", Some(1), None)],
    )?;
    let r = fx.read("stats", vec![("g", text("c"))]);
    ensure!(
        matches!(r, Err(DataPlaneError::Evaluation(EvalError::IntOverflow))),
        "final overflow gave {r:?}"
    );
    Ok(())
}

fn agg_3(fx: &Fixture) -> T {
    let xs = [1e15, 1.0, -1e15, 3.25, -3.25];
    put_nums(
        fx,
        xs.iter()
            .enumerate()
            .map(|(i, x)| (i as i64 + 1, "f", None, Some(*x)))
            .collect(),
    )?;
    let r = ok(fx.read("stats", vec![("g", text("f"))]), "read")?;
    let Value::Float(sum) = one_row(&r)?[&f("sum_x")] else {
        return Err(Fail::Failed("sum_x not Float".into()));
    };
    let n = xs.len() as f64;
    let bound = 2.0 * n * 2f64.powi(-53) * xs.iter().map(|x| x.abs()).sum::<f64>();
    ensure!(
        (sum - 1.0).abs() <= bound,
        "sum {sum} outside the bound {bound}"
    );
    Ok(())
}

fn agg_4(fx: &Fixture) -> T {
    let mut id = 0;
    for n in [1i64, 2, 3, 100] {
        let rows = (1..=n)
            .map(|v| {
                id += 1;
                (id, "", Some(v), None)
            })
            .collect::<Vec<_>>();
        let g = format!("n{n}");
        let rows: Vec<_> = rows
            .into_iter()
            .map(|(i, _, v, x)| (i, g.as_str(), v, x))
            .collect();
        put_nums(fx, rows)?;
    }
    for n in [1u64, 2, 3, 100] {
        for p in PCTS {
            let rank = ((*p as u128 * n as u128).div_ceil(1_000_000) as i64).max(1);
            let r = ok(
                fx.read(&format!("pct_{p}"), vec![("g", text(&format!("n{n}")))]),
                "read",
            )?;
            let got = &one_row(&r)?[&f("p")];
            ensure!(
                *got == Value::Int(rank),
                "p={p} n={n}: got {got:?}, want rank {rank}"
            );
        }
    }
    Ok(())
}

// --------------------------------------------------------- index and scan

fn idx_1(fx: &Fixture) -> T {
    fx.put_kv(1, vec![], RowConflict::Skip).ok();
    let m = Mutate::new(
        "kv",
        Selection::Where {
            predicate: Expr::eq(Expr::field("b"), Expr::boolean(true)),
        },
        Action::Set(vec![(f("i"), Expr::int(1))]),
        10,
    );
    let r = fx.commit(vec![WriteOp::Mutate(m)]);
    ensure!(
        matches!(r, Err(DataPlaneError::SelectorNotIndexed)),
        "unindexed Where gave {r:?}"
    );
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f("i"), Expr::int(1))]),
        10,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "Keys")?;
    ensure!(
        r.status == CommitStatus::Committed,
        "Keys selection needs no access term"
    );
    Ok(())
}

fn small_scan(fx: &Fixture) -> Result<u64, Fail> {
    let n = fx.limits().max_writer_scan_rows;
    if n > 2000 {
        return Err(Fail::Skipped(format!(
            "max_writer_scan_rows {n} too large for this test; use test limits"
        )));
    }
    Ok(n)
}

fn fill_kv(fx: &Fixture, n: u64, s: &str) -> T {
    let rows = (0..n as i64)
        .map(|k| {
            [
                (f("k"), Value::Int(100 + k)),
                (f("s"), text(s)),
                (f("i"), Value::Int(k)),
            ]
            .into_iter()
            .collect()
        })
        .collect();
    let r = ok(
        fx.commit(vec![WriteOp::PutRows {
            table: o("kv"),
            rows,
            on_conflict: RowConflict::Replace,
        }]),
        "fill",
    )?;
    ensure!(r.status == CommitStatus::Committed, "fill rejected");
    Ok(())
}

fn scan_1(fx: &Fixture) -> T {
    let n = small_scan(fx)?;
    fill_kv(fx, n + 10, "x")?;
    let m = Mutate::new(
        "kv",
        Selection::Where {
            predicate: Expr::And(vec![
                Expr::eq(Expr::field("s"), Expr::text("x")),
                Expr::eq(Expr::field("i"), Expr::int(-5)),
            ]),
        },
        Action::Set(vec![(f("b"), Expr::boolean(true))]),
        10,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "commit")?;
    ensure!(
        r.status == CommitStatus::Rejected
            && r.failure.as_ref().map(|x| &x.reason) == Some(&FailureReason::ScanLimitExceeded),
        "expected ScanLimitExceeded, got {r:?}"
    );
    Ok(())
}

fn scan_2(fx: &Fixture) -> T {
    let n = small_scan(fx)?;
    fill_kv(fx, n * 2 + 5, "y")?;
    let mut m = Mutate::new(
        "kv",
        Selection::Where {
            predicate: Expr::And(vec![
                Expr::eq(Expr::field("s"), Expr::text("y")),
                Expr::eq(Expr::field("i"), Expr::int(-5)),
            ]),
        },
        Action::Set(vec![(f("b"), Expr::boolean(true))]),
        10,
    );
    m.chunk = Some(Chunk::Start);
    let mut steps = 0;
    loop {
        steps += 1;
        ensure!(steps < 20, "chunking did not finish");
        let r = ok(fx.commit(vec![WriteOp::Mutate(m.clone())]), "step")?;
        ensure!(
            r.status == CommitStatus::Committed,
            "step rejected: {:?}",
            r.failure
        );
        match r.continuation {
            Some(c) => m.chunk = Some(Chunk::Continue(c)),
            None => break,
        }
    }
    ensure!(
        steps >= 3,
        "expected at least 3 steps over {} rows with budget {n}, got {steps}",
        n * 2 + 5
    );
    Ok(())
}

fn block_gate(fx: &Fixture, g: &str, state: GateState) -> T {
    let r = ok(
        fx.commit(vec![WriteOp::SetGates {
            queue: o("work"),
            gates: vec![(g.into(), state)],
        }]),
        "gates",
    )?;
    ensure!(r.status == CommitStatus::Committed, "gates rejected");
    Ok(())
}

fn scan_3(fx: &Fixture) -> T {
    let n = small_scan(fx)?;
    let items = (0..n as i64 + 5)
        .map(|i| {
            let mut it = NewItem::new(Some(&format!("g{i}")), PriorityValue::Int(i));
            it.gate_keys.insert("g1".into());
            it
        })
        .collect();
    ok(fx.enqueue(items, KeyConflict::Duplicate), "enqueue")?;
    fx.push("free", 10_000);
    block_gate(fx, "g1", GateState::Blocked)?;
    let r = ok(fx.claim(1, 10_000), "claim")?;
    ensure!(
        r.scan_limited && r.items.is_empty(),
        "expected an empty, scan-limited claim: {} items, limited {}",
        r.items.len(),
        r.scan_limited
    );
    Ok(())
}

fn scan_4(fx: &Fixture) -> T {
    for (k, i) in [(1, 30), (2, 10), (3, 20)] {
        ok(
            fx.put_kv(
                k,
                vec![("s", text("p")), ("i", Value::Int(i))],
                RowConflict::Replace,
            ),
            "put",
        )?;
    }
    let mut got = vec![];
    let mut token = None;
    for _ in 0..5 {
        let r = ok(
            block_on(fx.plane.read(ReadRequest {
                space: sp(),
                name: ReadName::new("rows_by_s"),
                version: 1,
                params: [(ParamName::new("s"), text("p"))].into_iter().collect(),
                page_token: token.clone(),
            })),
            "read",
        )?;
        for row in &r.rows {
            got.push(row[&f("i")].clone());
        }
        token = r.next_page_token;
        if token.is_none() {
            break;
        }
    }
    ensure!(
        got == vec![Value::Int(10), Value::Int(20), Value::Int(30)],
        "pages not in order: {got:?}"
    );
    Ok(())
}

fn scan_5(fx: &Fixture) -> T {
    let mk = |k: &str, p: i64, c: &str| {
        let mut it = NewItem::new(Some(k), PriorityValue::Int(p));
        it.fields.insert(f("campaign"), text(c));
        it
    };
    ok(
        fx.enqueue(
            vec![
                mk("a", 5, "c"),
                mk("b", 3, "c"),
                mk("c", 9, "c"),
                mk("d", 1, "d"),
            ],
            KeyConflict::Duplicate,
        ),
        "enqueue",
    )?;
    let mut r = ClaimRequest::new(fx.hdr(), "work", "w", 2, 10_000);
    r.filter = Some(Expr::eq(Expr::field("campaign"), Expr::text("c")));
    let resp = ok(fx.claim_req(r), "claim")?;
    let prios: Vec<_> = resp.items.iter().map(|i| i.priority).collect();
    ensure!(
        prios == vec![PriorityValue::Int(3), PriorityValue::Int(5)],
        "strict filtered claim gave {prios:?}"
    );
    let mut r = ClaimRequest::new(fx.hdr(), "work", "w", 2, 10_000);
    r.filter = Some(Expr::eq(Expr::field("n"), Expr::int(1)));
    let bad = fx.claim_req(r);
    ensure!(
        matches!(bad, Err(DataPlaneError::SelectorNotIndexed)),
        "a Range-only claim filter gave {bad:?}"
    );
    Ok(())
}

fn lim_1(fx: &Fixture) -> T {
    let l = fx.limits();
    let ops = (0..=l.max_ops_per_commit)
        .map(|_| WriteOp::SetQueueState {
            queue: o("work"),
            paused: None,
            drain_intake: None,
        })
        .collect();
    let h = fx.hdr();
    let r = fx.commit_with(h.clone(), ops, Params::new(), false);
    ensure!(
        matches!(r, Err(DataPlaneError::BatchTooLarge(LimitKind::Ops))),
        "ops over cap gave {r:?}"
    );
    let r = ok(
        fx.commit_with(
            h,
            vec![WriteOp::SetQueueState {
                queue: o("work"),
                paused: None,
                drain_intake: None,
            }],
            Params::new(),
            false,
        ),
        "same id after",
    )?;
    ensure!(
        r.status == CommitStatus::Committed,
        "validation failure left a record"
    );
    let list: Vec<Expr> = (0..=l.max_in_list as i64).map(Expr::int).collect();
    fx.put_kv(1, vec![], RowConflict::Skip).ok();
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(
            f("b"),
            Expr::In {
                expr: bx(Expr::int(1)),
                list,
            },
        )]),
        1,
    );
    let r = fx.commit(vec![WriteOp::Mutate(m)]);
    ensure!(
        matches!(r, Err(DataPlaneError::BatchTooLarge(LimitKind::InList))),
        "In over cap gave {r:?}"
    );
    let r = fx.claim(l.max_claim_batch_size + 1, 1000);
    ensure!(
        matches!(r, Err(DataPlaneError::BatchTooLarge(LimitKind::ClaimBatch))),
        "claim over cap gave {r:?}"
    );
    if l.max_entries_per_commit <= 20_000 {
        let rows = (0..=l.max_entries_per_commit as i64)
            .map(|k| [(f("k"), Value::Int(k))].into_iter().collect())
            .collect();
        let r = fx.commit(vec![WriteOp::PutRows {
            table: o("kv"),
            rows,
            on_conflict: RowConflict::Skip,
        }]);
        ensure!(
            matches!(r, Err(DataPlaneError::BatchTooLarge(LimitKind::Entries))),
            "entries over cap gave {r:?}"
        );
    }
    Ok(())
}

fn lim_2(fx: &Fixture) -> T {
    let max = fx.limits().max_body_bytes;
    if max > 1 << 20 {
        return skip("max_body_bytes too large for this test; use test limits");
    }
    let id = fx.push("big", 1);
    let chunk = "x".repeat((max / 2 + 16) as usize);
    let append = || BodyPatch::Append {
        pointer: JsonPointer::new("/log"),
        value: Expr::text(&chunk),
        keep_last: None,
    };
    let m = Mutate::new(
        "work",
        item_keys(&[id]),
        Action::Patch(vec![append(), append()]),
        1,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "commit")?;
    ensure!(
        r.status == CommitStatus::Rejected
            && r.failure.as_ref().map(|x| &x.reason)
                == Some(&FailureReason::Entry(EntryStatus::ValueTooLarge)),
        "growing a body past the limit gave {r:?}"
    );
    Ok(())
}

// ------------------------------------------------------------ group commit

fn reason(r: &CommitResponse) -> Option<&FailureReason> {
    r.failure.as_ref().map(|x| &x.reason)
}

fn put_op(k: i64) -> WriteOp {
    WriteOp::PutRows {
        table: o("kv"),
        rows: vec![[(f("k"), Value::Int(k))].into_iter().collect()],
        on_conflict: RowConflict::Reject,
    }
}

fn guarded(k: i64, v: u64) -> WriteOp {
    WriteOp::Mutate(Mutate::new(
        "kv",
        Selection::Keys {
            keys: vec![Keyed {
                key: KeyRef::Row(vec![Value::Int(k)]),
                expected_version: Some(v),
            }],
            filter: None,
        },
        Action::Set(vec![(f("i"), Expr::int(1))]),
        1,
    ))
}

fn grp_1(fx: &Fixture) -> T {
    let r = ok(fx.commit(vec![put_op(5), guarded(6, 1)]), "commit")?;
    ensure!(r.status == CommitStatus::Rejected, "expected rejection");
    ensure!(
        fx.get_row("kv", 5).is_none(),
        "an earlier operation of a rejected group took effect"
    );
    Ok(())
}

fn grp_2(fx: &Fixture) -> T {
    let ops = vec![
        WriteOp::Enqueue {
            queue: o("work"),
            items: vec![NewItem::new(Some("g2"), PriorityValue::Int(1))],
            on_key_conflict: KeyConflict::Reject,
        },
        WriteOp::Mutate(Mutate::new(
            "work",
            Selection::Keys {
                keys: vec![Keyed {
                    key: KeyRef::Item(ItemRef::Key(ClientItemKey::new("g2"))),
                    expected_version: None,
                }],
                filter: None,
            },
            Action::SetMetadata(
                [("x".to_string(), Some(serde_json::json!(1)))]
                    .into_iter()
                    .collect(),
            ),
            1,
        )),
    ];
    let r = ok(fx.commit(ops), "commit")?;
    ensure!(
        r.status == CommitStatus::Committed && r.ops[1].affected == 1,
        "later op did not see the enqueue: {r:?}"
    );
    Ok(())
}

fn grp_3(fx: &Fixture) -> T {
    ok(fx.commit(vec![put_op(9)]), "put")?;
    let r = ok(fx.commit(vec![put_op(9)]), "dup put")?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::Conflict)),
        "PutRows Reject: {r:?}"
    );
    fx.push("dupkey", 1);
    let r = ok(
        fx.commit(vec![WriteOp::Enqueue {
            queue: o("work"),
            items: vec![NewItem::new(Some("dupkey"), PriorityValue::Int(1))],
            on_key_conflict: KeyConflict::Reject,
        }]),
        "enqueue",
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::Conflict)),
        "Enqueue Reject: {r:?}"
    );
    let stale = FinalizeEntry::new(
        LeaseTarget::Item(LeaseRef {
            item_id: ItemId(999_999),
            lease_token: LeaseToken::new("nope"),
        }),
        FinalizeOutcome::Complete,
    );
    let r = ok(
        fx.commit(vec![WriteOp::Finalize {
            queue: o("work"),
            entries: vec![stale],
        }]),
        "finalize",
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::StaleLease)),
        "stale finalize: {r:?}"
    );
    let mut m = Mutate::new(
        "kv",
        keys(&[9]),
        Action::Set(vec![(f("i"), Expr::int(1))]),
        1,
    );
    m.expect = Some(MatchExpect { min: 2, max: 2 });
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "expect")?;
    ensure!(
        reason(&r) == Some(&FailureReason::MatchExpectUnmet { matched: 1 }),
        "expect: {r:?}"
    );
    ok(fx.commit(vec![put_op(10)]), "put")?;
    let m = Mutate::new(
        "kv",
        keys(&[9, 10]),
        Action::Set(vec![(f("i"), Expr::int(1))]),
        1,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "limit")?;
    ensure!(
        matches!(reason(&r), Some(FailureReason::LimitExceeded { .. })),
        "limit: {r:?}"
    );
    Ok(())
}

fn grp_4(fx: &Fixture) -> T {
    let r = ok(
        fx.commit(vec![put_op(11), guarded(12, 1), put_op(13)]),
        "commit",
    )?;
    let fl = r
        .failure
        .clone()
        .ok_or_else(|| Fail::Failed("no failure".into()))?;
    ensure!(
        fl.op_index == 1 && fl.entry_index == Some(0),
        "failure {fl:?}"
    );
    ensure!(
        r.ops[0].evaluated && r.ops[1].evaluated && !r.ops[2].evaluated,
        "evaluated flags {:?}",
        r.ops
    );
    Ok(())
}

fn grp_5(fx: &Fixture) -> T {
    fx.push("v1", 1);
    let c = ok(fx.claim(1, 60_000), "claim")?;
    let good = FinalizeEntry::new(lease_of(&c.items[0]), FinalizeOutcome::Complete);
    let stale = FinalizeEntry::new(
        LeaseTarget::Item(LeaseRef {
            item_id: c.items[0].item_id,
            lease_token: LeaseToken::new("wrong"),
        }),
        FinalizeOutcome::Complete,
    );
    let r = ok(
        fx.commit(vec![WriteOp::Finalize {
            queue: o("work"),
            entries: vec![stale.clone(), good.clone()],
        }]),
        "group",
    )?;
    ensure!(
        r.status == CommitStatus::Rejected,
        "a stale lease must reject the group"
    );
    let v = ok(fx.finalize(vec![stale, good]), "verb")?;
    ensure!(
        v.entries[0].status == EntryStatus::StaleLease
            && v.entries[1].status == EntryStatus::Completed,
        "verb outcomes {:?}",
        v.entries
    );
    Ok(())
}

fn meta_set() -> Action {
    Action::SetMetadata(
        [("m".to_string(), Some(serde_json::json!(true)))]
            .into_iter()
            .collect(),
    )
}

fn grp_6(fx: &Fixture) -> T {
    let id = fx.push("lp", 1);
    let c = ok(fx.claim(1, 60_000), "claim")?;
    let token = c.items[0].lease_token.clone().unwrap();
    let mk = |action: Action, policy: LeasePolicy| {
        let mut m = Mutate::new("work", item_keys(&[id]), action, 1);
        m.lease_policy = policy;
        WriteOp::Mutate(m)
    };
    let r = ok(fx.commit(vec![mk(meta_set(), LeasePolicy::Skip)]), "skip")?;
    ensure!(
        r.ops[0].skipped_leased == 1 && r.ops[0].affected == 0,
        "Skip: {:?}",
        r.ops[0]
    );
    let r = ok(
        fx.commit(vec![mk(meta_set(), LeasePolicy::Reject)]),
        "reject",
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::LeasedMatch),
        "Reject: {r:?}"
    );
    let r = ok(
        fx.commit(vec![mk(meta_set(), LeasePolicy::Preserve)]),
        "preserve",
    )?;
    let view = fx.get_item(id, true).unwrap();
    ensure!(
        r.ops[0].affected == 1 && view.state == ItemState::Leased,
        "Preserve: {r:?} {view:?}"
    );
    let bad = fx.commit(vec![mk(Action::Retire, LeasePolicy::Preserve)]);
    ensure!(
        matches!(bad, Err(DataPlaneError::InvalidRequest(_))),
        "Preserve with Retire gave {bad:?}"
    );
    let r = ok(
        fx.commit(vec![mk(
            Action::Transition(Transition::Complete { code: None }),
            LeasePolicy::MatchToken(LeaseToken::new("x")),
        )]),
        "wrong token",
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::StaleLease)),
        "MatchToken mismatch: {r:?}"
    );
    let r = ok(
        fx.commit(vec![mk(
            Action::Transition(Transition::Complete { code: None }),
            LeasePolicy::MatchToken(token),
        )]),
        "token",
    )?;
    ensure!(r.status == CommitStatus::Committed, "MatchToken: {r:?}");
    let id2 = fx.push("lp2", 1);
    let c2 = ok(fx.claim(1, 60_000), "claim 2")?;
    let r = ok(
        fx.commit(vec![{
            let mut m = Mutate::new("work", item_keys(&[id2]), Action::Retire, 1);
            m.lease_policy = LeasePolicy::Break;
            WriteOp::Mutate(m)
        }]),
        "break",
    )?;
    ensure!(r.status == CommitStatus::Committed, "Break: {r:?}");
    let v = ok(
        fx.finalize(vec![FinalizeEntry::new(
            lease_of(&c2.items[0]),
            FinalizeOutcome::Complete,
        )]),
        "finalize",
    )?;
    ensure!(
        v.entries[0].status != EntryStatus::Completed,
        "a broken lease's token still worked"
    );
    Ok(())
}

fn grp_7(fx: &Fixture) -> T {
    ok(
        fx.put_kv(
            1,
            vec![("s", text("a")), ("u", text("b"))],
            RowConflict::Replace,
        ),
        "put",
    )?;
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f("s"), Expr::field("u")), (f("u"), Expr::field("s"))]),
        1,
    );
    ok(fx.commit(vec![WriteOp::Mutate(m)]), "swap")?;
    let row = fx.get_row("kv", 1).unwrap();
    ensure!(
        row.fields[&f("s")] == text("b") && row.fields[&f("u")] == text("a"),
        "swap gave {:?}",
        row.fields
    );
    let m = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f("s"), Expr::text("x")), (f("s"), Expr::text("y"))]),
        1,
    );
    let r = fx.commit(vec![WriteOp::Mutate(m)]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "duplicate assignment gave {r:?}"
    );
    Ok(())
}

fn grp_8(fx: &Fixture) -> T {
    ok(fx.put_kv(1, vec![], RowConflict::Skip), "put")?;
    let m = Mutate::new(
        "kv",
        keys(&[1, 1]),
        Action::Set(vec![(f("i"), Expr::int(1))]),
        1,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "dup keys")?;
    ensure!(
        r.status == CommitStatus::Committed && r.ops[0].matched == 1,
        "repeated key: {r:?}"
    );
    let id = fx.push("cnt", 1);
    ok(fx.claim(1, 60_000), "claim")?;
    let mut m = Mutate::new("work", item_keys(&[id]), meta_set(), 1);
    m.expect = Some(MatchExpect { min: 1, max: 1 });
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "leased count")?;
    ensure!(
        r.status == CommitStatus::Committed
            && r.ops[0].matched == 1
            && r.ops[0].skipped_leased == 1,
        "leased skip counts in matched: {:?}",
        r.ops[0]
    );
    Ok(())
}

fn grp_9(fx: &Fixture) -> T {
    if !fx.caps().queue_features.contains(&QueueFeature::Cohorts) {
        return skip("Cohorts not declared");
    }
    Err(Fail::Failed(
        "Cohorts declared but GRP-9 not written yet".into(),
    ))
}

fn grp_13(fx: &Fixture) -> T {
    grp_9(fx)
}

fn coh_1(fx: &Fixture) -> T {
    grp_9(fx)
}

fn grp_10(fx: &Fixture) -> T {
    let id = fx.push("ver", 1);
    ensure!(
        fx.get_item(id, false).unwrap().item_version == 1,
        "new item version"
    );
    let c = ok(fx.claim(1, 1000), "claim")?;
    ensure!(
        c.items[0].item_version == 2,
        "claim version {}",
        c.items[0].item_version
    );
    let r = ok(
        block_on(fx.plane.renew(RenewRequest {
            header: fx.hdr(),
            queue: o("work"),
            leases: vec![lease_of(&c.items[0])],
            lease_duration_ms: 1000,
        })),
        "renew",
    )?;
    let nl = r.entries[0]
        .lease
        .clone()
        .ok_or_else(|| Fail::Failed("renew returned no lease".into()))?;
    ensure!(nl.item_version == 3, "renew version {}", nl.item_version);
    fx.advance(2000);
    let v = fx.get_item(id, false).unwrap();
    ensure!(
        v.state == ItemState::Pending && v.item_version == 3,
        "lease expiry changed the version: {v:?}"
    );
    ok(
        fx.put_kv(1, vec![("i", Value::Int(1))], RowConflict::Replace),
        "put",
    )?;
    let v1 = fx.get_row("kv", 1).unwrap().row_version;
    ok(
        fx.put_kv(1, vec![("i", Value::Int(2))], RowConflict::Merge),
        "merge",
    )?;
    let v2 = fx.get_row("kv", 1).unwrap().row_version;
    ok(
        fx.put_kv(1, vec![("i", Value::Int(2))], RowConflict::Merge),
        "no-op merge",
    )?;
    let v3 = fx.get_row("kv", 1).unwrap().row_version;
    ensure!(v2 == v1 + 1 && v3 == v2, "row versions {v1} {v2} {v3}");
    Ok(())
}

fn grp_11(fx: &Fixture) -> T {
    ok(fx.commit(vec![put_op(7)]), "put")?;
    let m = Mutate::new(
        "kv",
        keys(&[7]),
        Action::Set(vec![(f("i"), Expr::int(1))]),
        1,
    );
    ok(fx.commit(vec![WriteOp::Mutate(m)]), "set")?;
    let old = fx.get_row("kv", 7).unwrap().row_version;
    ok(
        fx.commit(vec![WriteOp::Mutate(Mutate::new(
            "kv",
            keys(&[7]),
            Action::Retire,
            1,
        ))]),
        "retire",
    )?;
    ensure!(fx.get_row("kv", 7).is_none(), "row not deleted");
    ok(fx.commit(vec![put_op(7)]), "reinsert")?;
    let new = fx.get_row("kv", 7).unwrap().row_version;
    ensure!(new > old, "re-inserted version {new} not above {old}");
    let r = ok(fx.commit(vec![guarded(7, old)]), "old guard")?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::VersionMismatch)),
        "old guard: {r:?}"
    );
    Ok(())
}

fn grp_12(fx: &Fixture) -> T {
    ok(fx.commit(vec![put_op(8)]), "put")?;
    let r = ok(fx.commit(vec![guarded(8, 0)]), "Some(0) on existing")?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::VersionMismatch)),
        "Some(0): {r:?}"
    );
    let r = ok(fx.commit(vec![guarded(4242, 3)]), "Some(3) on absent")?;
    ensure!(
        reason(&r) == Some(&FailureReason::Entry(EntryStatus::VersionMismatch)),
        "Some(3): {r:?}"
    );
    Ok(())
}

fn grp_14(fx: &Fixture) -> T {
    ok(
        fx.put_kv(1, vec![("u", text("one"))], RowConflict::Replace),
        "put 1",
    )?;
    ok(fx.put_kv(2, vec![], RowConflict::Replace), "put 2")?;
    let m = Mutate::new(
        "kv",
        keys(&[2]),
        Action::Set(vec![(f("u"), Expr::text("one"))]),
        1,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "unique")?;
    ensure!(
        matches!(
            reason(&r),
            Some(FailureReason::Entry(EntryStatus::UniqueViolation { .. }))
        ),
        "unique: {r:?}"
    );
    let row: BTreeMap<FieldName, Value> = [(f("k"), Value::Int(1)), (f("req"), text("x"))]
        .into_iter()
        .collect();
    ok(
        fx.commit(vec![WriteOp::PutRows {
            table: o("strict"),
            rows: vec![row],
            on_conflict: RowConflict::Reject,
        }]),
        "strict",
    )?;
    let m = Mutate::new(
        "strict",
        keys(&[1]),
        Action::Set(vec![(f("req"), Expr::null(ScalarType::Text))]),
        1,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "null")?;
    ensure!(
        matches!(
            reason(&r),
            Some(FailureReason::Entry(EntryStatus::MissingField { .. }))
        ),
        "non-null: {r:?}"
    );
    Ok(())
}

// ------------------------------------------------------------- patch etc.

fn body_of(fx: &Fixture, id: ItemId) -> Option<serde_json::Value> {
    match fx.get_item(id, true)?.body {
        Some(Body::Json(j)) => Some(j),
        _ => None,
    }
}

fn patch(fx: &Fixture, id: ItemId, ps: Vec<BodyPatch>) -> Result<CommitResponse, Fail> {
    ok(
        fx.commit(vec![WriteOp::Mutate(Mutate::new(
            "work",
            item_keys(&[id]),
            Action::Patch(ps),
            1,
        ))]),
        "patch",
    )
}

fn ptr(p: &str) -> JsonPointer {
    JsonPointer::new(p)
}

fn jv(v: serde_json::Value) -> Expr {
    lit(Value::Json(v))
}

fn patch_1(fx: &Fixture) -> T {
    use serde_json::json;
    let id = fx.push("p", 1);
    patch(
        fx,
        id,
        vec![BodyPatch::Set {
            pointer: ptr("/a/b"),
            value: Expr::int(1),
        }],
    )?;
    ensure!(
        body_of(fx, id) == Some(json!({"a": {"b": 1}})),
        "Set creates parents: {:?}",
        body_of(fx, id)
    );
    patch(
        fx,
        id,
        vec![BodyPatch::Set {
            pointer: ptr(""),
            value: jv(json!({"arr": [1, 2], "s": 5})),
        }],
    )?;
    ensure!(
        body_of(fx, id) == Some(json!({"arr": [1, 2], "s": 5})),
        "root Set"
    );
    let r = patch(
        fx,
        id,
        vec![BodyPatch::Set {
            pointer: ptr("/s/x"),
            value: Expr::int(1),
        }],
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::Evaluation(EvalError::PointerConflict)),
        "scalar parent: {r:?}"
    );
    patch(
        fx,
        id,
        vec![
            BodyPatch::Set {
                pointer: ptr("/arr/0"),
                value: Expr::int(9),
            },
            BodyPatch::Set {
                pointer: ptr("/arr/-"),
                value: Expr::int(3),
            },
            BodyPatch::Set {
                pointer: ptr("/arr/3"),
                value: Expr::int(4),
            },
        ],
    )?;
    ensure!(
        body_of(fx, id) == Some(json!({"arr": [9, 2, 3, 4], "s": 5})),
        "array Set: {:?}",
        body_of(fx, id)
    );
    let r = patch(
        fx,
        id,
        vec![BodyPatch::Set {
            pointer: ptr("/arr/9"),
            value: Expr::int(1),
        }],
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::Evaluation(EvalError::PointerConflict)),
        "index beyond len: {r:?}"
    );
    patch(
        fx,
        id,
        vec![BodyPatch::Remove {
            pointer: ptr("/nope"),
        }],
    )?;
    ensure!(
        body_of(fx, id) == Some(json!({"arr": [9, 2, 3, 4], "s": 5})),
        "Remove absent"
    );
    patch(
        fx,
        id,
        vec![
            BodyPatch::Append {
                pointer: ptr("/log"),
                value: Expr::int(1),
                keep_last: Some(2),
            },
            BodyPatch::Append {
                pointer: ptr("/log"),
                value: Expr::int(2),
                keep_last: Some(2),
            },
            BodyPatch::Append {
                pointer: ptr("/log"),
                value: Expr::int(3),
                keep_last: Some(2),
            },
        ],
    )?;
    ensure!(
        body_of(fx, id).and_then(|b| b.get("log").cloned()) == Some(json!([2, 3])),
        "Append keep_last"
    );
    let r = patch(
        fx,
        id,
        vec![BodyPatch::Append {
            pointer: ptr("/s"),
            value: Expr::int(1),
            keep_last: None,
        }],
    )?;
    ensure!(
        reason(&r) == Some(&FailureReason::Evaluation(EvalError::PointerConflict)),
        "Append non-array: {r:?}"
    );
    patch(fx, id, vec![BodyPatch::Remove { pointer: ptr("") }])?;
    ensure!(body_of(fx, id).is_none(), "root Remove");
    Ok(())
}

fn fin_1(fx: &Fixture) -> T {
    use serde_json::json;
    let id = fx.push("fin", 1);
    let c = ok(fx.claim(1, 60_000), "claim")?;
    let mut e = FinalizeEntry::new(lease_of(&c.items[0]), FinalizeOutcome::Complete);
    e.expected_item_version = Some(1);
    e.body = Some(Body::Json(json!({"x": 1})));
    let r = ok(fx.finalize(vec![e.clone()]), "wrong version")?;
    ensure!(
        r.entries[0].status == EntryStatus::VersionMismatch,
        "version guard: {:?}",
        r.entries[0]
    );
    ensure!(
        body_of(fx, id).is_none(),
        "a rejected finalize changed the body"
    );
    e.expected_item_version = Some(c.items[0].item_version);
    e.patch = vec![BodyPatch::Set {
        pointer: ptr("/y"),
        value: Expr::int(2),
    }];
    e.set = vec![(f("n"), Expr::int(7))];
    let r = ok(fx.finalize(vec![e]), "finalize")?;
    ensure!(
        r.entries[0].status == EntryStatus::Completed,
        "finalize: {:?}",
        r.entries[0]
    );
    let v = fx.get_item(id, true).unwrap();
    ensure!(
        v.body == Some(Body::Json(json!({"x": 1, "y": 2}))),
        "body then patch: {:?}",
        v.body
    );
    ensure!(
        v.fields.get(&f("n")) == Some(&Value::Int(7)),
        "set applied: {:?}",
        v.fields
    );
    Ok(())
}

fn chunk_mutate(pred: Expr, action: Action) -> Mutate {
    let mut m = Mutate::new("kv", Selection::Where { predicate: pred }, action, 5);
    m.chunk = Some(Chunk::Start);
    m
}

fn chk_1(fx: &Fixture) -> T {
    fill_kv(fx, 30, "z")?;
    let pred = Expr::eq(Expr::field("s"), Expr::text("z"));
    let m = chunk_mutate(
        pred.clone(),
        Action::Set(vec![(f("b"), Expr::boolean(true))]),
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "start")?;
    let c = r
        .continuation
        .ok_or_else(|| Fail::Failed("no continuation after the first step".into()))?;
    let mut changed = chunk_mutate(pred, Action::Set(vec![(f("b"), Expr::boolean(false))]));
    changed.chunk = Some(Chunk::Continue(c));
    let r = fx.commit(vec![WriteOp::Mutate(changed)]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidContinuation)),
        "changed action gave {r:?}"
    );
    Ok(())
}

fn chk_2(fx: &Fixture) -> T {
    let pred = Expr::eq(Expr::field("s"), Expr::text("z"));
    let set_b = Action::Set(vec![(f("b"), Expr::boolean(true))]);
    let r = fx.commit(vec![
        WriteOp::Mutate(chunk_mutate(pred.clone(), set_b.clone())),
        put_op(1),
    ]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "chunk with another op gave {r:?}"
    );
    let mut m = Mutate::new("kv", keys(&[1]), set_b, 5);
    m.chunk = Some(Chunk::Start);
    let r = fx.commit(vec![WriteOp::Mutate(m)]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "chunk with Keys gave {r:?}"
    );
    let r = fx.commit(vec![WriteOp::Mutate(chunk_mutate(
        pred,
        Action::Set(vec![(f("s"), Expr::text("q"))]),
    ))]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "chunk changing its access field gave {r:?}"
    );
    let qm = |states: &[&str], tr: Transition| {
        let pred = if states.len() == 1 {
            Expr::eq(Expr::field("fw_state"), Expr::text(states[0]))
        } else {
            Expr::In {
                expr: bx(Expr::field("fw_state")),
                list: states.iter().map(|s| Expr::text(s)).collect(),
            }
        };
        let mut m = Mutate::new(
            "work",
            Selection::Where { predicate: pred },
            Action::Transition(tr),
            5,
        );
        m.chunk = Some(Chunk::Start);
        WriteOp::Mutate(m)
    };
    let r = fx.commit(vec![qm(&["pending"], Transition::Complete { code: None })]);
    ensure!(
        matches!(r, Ok(ref x) if x.status == CommitStatus::Committed),
        "out of the access state gave {r:?}"
    );
    let r = fx.commit(vec![qm(&["failed"], Transition::Redrive)]);
    ensure!(
        matches!(r, Ok(ref x) if x.status == CommitStatus::Committed),
        "failed -> pending gave {r:?}"
    );
    let r = fx.commit(vec![qm(&["failed", "pending"], Transition::Redrive)]);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "back into the access state gave {r:?}"
    );
    Ok(())
}

fn dry_1(fx: &Fixture) -> T {
    fill_kv(fx, 6, "d")?;
    let m = Mutate::new(
        "kv",
        Selection::Where {
            predicate: Expr::eq(Expr::field("s"), Expr::text("d")),
        },
        Action::Set(vec![(f("b"), Expr::boolean(true))]),
        100,
    );
    let h = fx.hdr();
    let dry = ok(
        fx.commit_with(
            h.clone(),
            vec![WriteOp::Mutate(m.clone())],
            Params::new(),
            true,
        ),
        "dry",
    )?;
    ensure!(
        dry.status == CommitStatus::DryRun && dry.ops[0].matched == 6,
        "dry run: {dry:?}"
    );
    ensure!(
        fx.get_row("kv", 100).unwrap().fields[&f("b")] == Value::Null(ScalarType::Bool),
        "dry run applied"
    );
    let real = ok(
        fx.commit_with(h, vec![WriteOp::Mutate(m)], Params::new(), false),
        "real",
    )?;
    ensure!(
        real.status == CommitStatus::Committed && real.ops[0].matched == 6,
        "the id was used by the dry run: {real:?}"
    );
    Ok(())
}

// ----------------------------------------------------------------- records

fn rec_1(fx: &Fixture) -> T {
    let h = fx.hdr();
    let r1 = ok(
        fx.commit_with(h.clone(), vec![guarded(20, 1)], Params::new(), false),
        "first",
    )?;
    ensure!(r1.status == CommitStatus::Rejected, "expected rejection");
    ok(fx.commit(vec![put_op(20)]), "make it pass")?;
    let r2 = ok(
        fx.commit_with(h, vec![guarded(20, 1)], Params::new(), false),
        "replay",
    )?;
    ensure!(r2 == r1, "replay of a rejection re-evaluated: {r2:?}");
    Ok(())
}

fn rec_2(fx: &Fixture) -> T {
    let h = fx.hdr();
    let bad = Mutate::new(
        "kv",
        keys(&[1]),
        Action::Set(vec![(f("i"), Expr::text("x"))]),
        1,
    );
    let r = fx.commit_with(h.clone(), vec![WriteOp::Mutate(bad)], Params::new(), false);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "expected validation failure"
    );
    let r = ok(
        fx.commit_with(h, vec![put_op(30)], Params::new(), false),
        "resend",
    )?;
    ensure!(
        r.status == CommitStatus::Committed,
        "the failed validation left a record"
    );
    Ok(())
}

fn rec_3(fx: &Fixture) -> T {
    let h = fx.hdr();
    let r1 = ok(
        fx.commit_with(h.clone(), vec![put_op(31)], Params::new(), false),
        "first",
    )?;
    let mut h2 = h.clone();
    h2.min_durability = Some(fx.plane.guarantees().durability);
    let r2 = ok(
        fx.commit_with(h2, vec![put_op(31)], Params::new(), false),
        "durability changed",
    )?;
    ensure!(
        r2.status == r1.status && r2.ops == r1.ops,
        "min_durability changed the replay"
    );
    let r3 = fx.commit_with(h, vec![put_op(32)], Params::new(), false);
    ensure!(
        matches!(r3, Err(DataPlaneError::RequestIdConflict)),
        "different body gave {r3:?}"
    );
    Ok(())
}

fn rec_6(fx: &Fixture) -> T {
    fx.push("c1", 1);
    let h = fx.hdr();
    let req = ClaimRequest::new(h.clone(), "work", "w", 1, 60_000);
    let c1 = ok(fx.claim_req(req.clone()), "claim")?;
    let c2 = ok(fx.claim_req(req.clone()), "replay")?;
    ensure!(c1 == c2, "replay while active differs");
    ok(
        fx.finalize(vec![FinalizeEntry::new(
            lease_of(&c1.items[0]),
            FinalizeOutcome::Complete,
        )]),
        "finalize",
    )?;
    let c3 = fx.claim_req(req);
    ensure!(
        matches!(c3, Err(DataPlaneError::RequestExpired)),
        "replay after the lease ended gave {c3:?}"
    );
    let empty = ClaimRequest::new(fx.hdr(), "work", "w", 1, 60_000);
    let e1 = ok(fx.claim_req(empty.clone()), "empty")?;
    fx.push("c2", 1);
    let e2 = ok(fx.claim_req(empty), "empty replay")?;
    ensure!(
        e1.items.is_empty() && e2 == e1,
        "an empty claim must replay empty"
    );
    Ok(())
}

fn rec_7(fx: &Fixture) -> T {
    fx.push("m1", 1);
    let c = ok(fx.claim(1, 60_000), "claim")?;
    let req = FinalizeRequest {
        header: fx.hdr(),
        queue: o("work"),
        entries: vec![
            FinalizeEntry::new(lease_of(&c.items[0]), FinalizeOutcome::Complete),
            FinalizeEntry::new(
                LeaseTarget::Item(LeaseRef {
                    item_id: ItemId(424_242),
                    lease_token: LeaseToken::new("x"),
                }),
                FinalizeOutcome::Complete,
            ),
        ],
        params: Params::new(),
    };
    let a = ok(block_on(fx.plane.finalize(req.clone())), "finalize")?;
    let b = ok(block_on(fx.plane.finalize(req)), "replay")?;
    ensure!(
        a == b && a.entries[0].status == EntryStatus::Completed,
        "verb replay differs"
    );
    Ok(())
}

fn rec_8(fx: &Fixture) -> T {
    fx.push("t1", 1);
    let h = fx.hdr();
    let req = ClaimRequest::new(h.clone(), "work", "w", 1, 60_000);
    let c = ok(fx.claim_req(req.clone()), "claim")?;
    let rec = ok(
        block_on(fx.plane.get_request(GetRequestRequest {
            space: sp(),
            request_id: h.request_id.clone(),
        })),
        "get_request",
    )?
    .ok_or_else(|| Fail::Failed("no record".into()))?;
    let RecordedResponse::Claim(rc) = rec.response else {
        return Err(Fail::Failed("not a claim record".into()));
    };
    ensure!(
        rc.items[0].lease_token != c.items[0].lease_token,
        "get_request returned a lease token"
    );
    let again = ok(fx.claim_req(req), "replay")?;
    ensure!(
        again.items[0].lease_token == c.items[0].lease_token,
        "replay did not return the token"
    );
    Ok(())
}

// -------------------------------------------------------------------- time

fn time_1(fx: &Fixture) -> T {
    let id = fx.push("exp", 1);
    let c = ok(fx.claim(1, 1000), "claim")?;
    let v = c.items[0].item_version;
    fx.advance(999);
    ensure!(
        fx.get_item(id, false).unwrap().state == ItemState::Leased,
        "expired early"
    );
    fx.advance(1);
    let it = fx.get_item(id, false).unwrap();
    ensure!(
        it.state == ItemState::Pending && it.lease.is_none() && it.item_version == v,
        "lease expiry: {it:?}"
    );
    let row: BTreeMap<FieldName, Value> = [(f("k"), Value::Int(1)), (f("req"), text("x"))]
        .into_iter()
        .collect();
    ok(
        fx.commit(vec![WriteOp::PutRows {
            table: o("strict"),
            rows: vec![row],
            on_conflict: RowConflict::Reject,
        }]),
        "put",
    )?;
    fx.advance(9_999);
    ensure!(fx.get_row("strict", 1).is_some(), "row expired early");
    fx.advance(1);
    ensure!(
        fx.get_row("strict", 1).is_none(),
        "row did not expire at its instant"
    );
    let h = fx.hdr();
    ok(
        fx.commit_with(h.clone(), vec![put_op(40)], Params::new(), false),
        "first",
    )?;
    fx.advance(600_000);
    let r = ok(
        fx.commit_with(h, vec![put_op(41)], Params::new(), false),
        "after retention",
    )?;
    ensure!(
        r.status == CommitStatus::Committed,
        "a forgotten request id did not run as new: {r:?}"
    );
    Ok(())
}

fn time_4(fx: &Fixture) -> T {
    let mut it = NewItem::new(Some("later"), PriorityValue::Int(1));
    it.not_before = Some(Schedule::At(UtcMillis(START + 5_000)));
    ok(fx.enqueue(vec![it], KeyConflict::Duplicate), "enqueue")?;
    let r = ok(fx.claim(1, 1000), "claim now")?;
    ensure!(r.items.is_empty(), "claimed before not_before");
    let mut req = ClaimRequest::new(fx.hdr(), "work", "w", 1, 1000);
    req.eligibility_time = Some(UtcMillis(START + 6_000));
    let r = ok(fx.claim_req(req), "claim at eligibility time")?;
    ensure!(
        r.items.len() == 1,
        "eligibility_time did not make the item eligible"
    );
    let exp = r.items[0].lease_expires_at.0;
    ensure!(
        exp == r.as_of.0 + 1000,
        "lease expiry {exp} not from the authority ({})",
        r.as_of.0
    );
    Ok(())
}

fn time_5(fx: &Fixture) -> T {
    let id = fx.push("lazy", 1);
    ok(fx.claim(1, 500), "claim")?;
    fx.advance(500);
    let r = ok(
        block_on(fx.plane.claim_by_item_ids(ClaimByItemIdsRequest {
            header: fx.hdr(),
            queue: o("work"),
            worker_id: WorkerId::new("w2"),
            item_ids: vec![id],
            lease_duration_ms: 1000,
            eligibility_time: None,
        })),
        "claim by id",
    )?;
    ensure!(
        r.results[0].1.is_ok(),
        "expired lease not processed before the request: {:?}",
        r.results[0]
    );
    Ok(())
}

fn dur_1(fx: &Fixture) -> T {
    let g = fx.plane.guarantees().durability;
    let max = fx.caps().max_durability;
    let in_range = |l: DurabilityLevel| l >= g && l <= max;
    let r = ok(fx.commit(vec![put_op(50)]), "commit")?;
    ensure!(
        r.durability.is_some_and(in_range),
        "commit durability {:?}",
        r.durability
    );
    let r = ok(fx.commit(vec![guarded(51, 1)]), "rejected")?;
    ensure!(
        r.durability.is_some_and(in_range),
        "a rejection reports no durability: {:?}",
        r.durability
    );
    let e = ok(
        fx.enqueue(
            vec![NewItem::new(Some("d"), PriorityValue::Int(1))],
            KeyConflict::Duplicate,
        ),
        "enqueue",
    )?;
    ensure!(
        in_range(e.durability),
        "enqueue durability {:?}",
        e.durability
    );
    let mut s = base_schema();
    s.version = 2;
    let a = ok(
        block_on(fx.plane.apply_schema(ApplySchemaRequest {
            space: sp(),
            schema: s,
            expected_version: Some(1),
            min_durability: None,
        })),
        "apply_schema",
    )?;
    ensure!(
        in_range(a.durability),
        "schema durability {:?}",
        a.durability
    );
    Ok(())
}

fn dur_2(fx: &Fixture) -> T {
    if fx.caps().max_durability >= DurabilityLevel::Replicated {
        return skip("Replicated is supported");
    }
    let mut h = fx.hdr();
    h.min_durability = Some(DurabilityLevel::Replicated);
    let r = fx.commit_with(h, vec![put_op(60)], Params::new(), false);
    ensure!(
        matches!(r, Err(DataPlaneError::InvalidRequest(_))),
        "min_durability above max gave {r:?}"
    );
    Ok(())
}

fn lin_2(fx: &Fixture) -> T {
    let r = ok(
        fx.commit(vec![
            WriteOp::Enqueue {
                queue: o("work"),
                items: vec![NewItem::new(Some("x1"), PriorityValue::Int(1))],
                on_key_conflict: KeyConflict::Reject,
            },
            put_op(70),
        ]),
        "commit",
    )?;
    let id = r.ops[0].entries[0].item_id.unwrap();
    ensure!(
        fx.get_item(id, false).is_some() && fx.get_row("kv", 70).is_some(),
        "a read after the commit missed it"
    );
    Ok(())
}

fn sch_1(fx: &Fixture) -> T {
    let apply = |s: SpaceSchema, ev| {
        block_on(fx.plane.apply_schema(ApplySchemaRequest {
            space: sp(),
            schema: s,
            expected_version: ev,
            min_durability: None,
        }))
    };
    let same = ok(apply(base_schema(), Some(1)), "identical")?;
    ensure!(!same.changed, "identical schema changed");
    let mut s2 = base_schema();
    s2.version = 2;
    let r = apply(s2.clone(), Some(7));
    ensure!(
        matches!(r, Err(DataPlaneError::SchemaConflict { .. })),
        "wrong expected version gave {r:?}"
    );
    let mut s3 = s2.clone();
    s3.tables[0].fields.retain(|d| d.name.as_str() != "fl");
    let r = apply(s3, Some(1));
    ensure!(
        matches!(r, Err(DataPlaneError::SchemaConflict { .. })),
        "removing a field gave {r:?}"
    );
    let mut s4 = s2.clone();
    s4.tables[0].fields.push(FieldDecl::stored(
        "extra",
        ScalarType::Int,
        false,
        IndexHint::None,
    ));
    let r = apply(s4, Some(1));
    ensure!(
        matches!(r, Err(DataPlaneError::SchemaConflict { .. })),
        "adding a non-null field gave {r:?}"
    );
    if !fx.caps().shapes.contains(&ShapeKind::EventLog) {
        let mut s5 = s2.clone();
        s5.event_logs.push(EventLogDecl {
            name: o("events"),
            partition_key: f("p"),
            fields: vec![FieldDecl::stored(
                "p",
                ScalarType::Text,
                false,
                IndexHint::None,
            )],
            body: BodyKind::Json,
            retention_ms: 1000,
        });
        let r = apply(s5, Some(1));
        ensure!(
            matches!(r, Err(DataPlaneError::FeatureUnsupported(_))),
            "undeclared shape gave {r:?}"
        );
    }
    let mut s6 = s2;
    s6.tables[0].fields.push(FieldDecl::stored(
        "extra",
        ScalarType::Int,
        true,
        IndexHint::None,
    ));
    let r = ok(apply(s6, Some(1)), "additive")?;
    ensure!(r.changed && r.version == 2, "additive change: {r:?}");
    Ok(())
}

fn read_1(fx: &Fixture) -> T {
    let mut s = base_schema();
    s.version = 2;
    if let Some(r) = s.reads.iter_mut().find(|r| r.name.as_str() == "rows_by_s") {
        r.max_rows = 1;
    }
    let r = block_on(fx.plane.apply_schema(ApplySchemaRequest {
        space: sp(),
        schema: s,
        expected_version: Some(1),
        min_durability: None,
    }));
    ensure!(
        matches!(r, Err(DataPlaneError::SchemaConflict { .. })),
        "changing a declared read version gave {r:?}"
    );
    let a = fx.push("ra", 1);
    fx.push("rb", 2);
    ok(fx.claim(1, 60_000), "claim")?;
    let r = ok(
        fx.read(
            "fw_items",
            vec![("queue", text("work")), ("state", text("leased"))],
        ),
        "fw_items",
    )?;
    ensure!(
        r.rows.len() == 1 && r.rows[0][&f("fw_item_id")] == Value::Int(a.0 as i64),
        "fw_items by state: {:?}",
        r.rows
    );
    let r = ok(
        fx.read(
            "fw_items",
            vec![("queue", text("work")), ("worker_id", text("w1"))],
        ),
        "by worker",
    )?;
    ensure!(r.rows.len() == 1, "fw_items by worker: {}", r.rows.len());
    let r = ok(
        fx.read("fw_queue_counts", vec![("queue", text("work"))]),
        "counts",
    )?;
    let pending = r
        .rows
        .iter()
        .find(|row| row[&f("fw_state")] == text("pending"))
        .map(|row| row[&f("count")].clone());
    ensure!(
        pending == Some(Value::Int(1)),
        "fw_queue_counts pending {pending:?}"
    );
    let r = fx.read("nope", vec![]);
    ensure!(
        matches!(r, Err(DataPlaneError::ReadNotDeclared { .. })),
        "undeclared read gave {r:?}"
    );
    Ok(())
}

fn read_2(fx: &Fixture) -> T {
    ok(
        fx.put_kv(
            1,
            vec![("s", text("n")), ("i", Value::Int(5))],
            RowConflict::Replace,
        ),
        "put",
    )?;
    ok(
        fx.put_kv(2, vec![("s", text("n"))], RowConflict::Replace),
        "put null",
    )?;
    let r = ok(fx.read("rows_by_s", vec![("s", text("n"))]), "read")?;
    let first = r.rows.first().map(|row| row[&f("i")].clone());
    ensure!(
        first.as_ref().is_some_and(Value::is_null),
        "null did not sort first ascending: {:?}",
        r.rows
    );
    Ok(())
}

fn enq_1(fx: &Fixture) -> T {
    let a = fx.push("k1", 1);
    let r = ok(
        fx.enqueue(
            vec![NewItem::new(Some("k1"), PriorityValue::Int(1))],
            KeyConflict::Duplicate,
        ),
        "dup",
    )?;
    ensure!(
        r.entries[0].status == EntryStatus::Duplicate && r.entries[0].item_id == Some(a),
        "Duplicate: {:?}",
        r.entries[0]
    );
    let r = ok(
        fx.enqueue(
            vec![NewItem::new(Some("k1"), PriorityValue::Int(2))],
            KeyConflict::ReplacePending,
        ),
        "replace",
    )?;
    let e = &r.entries[0];
    ensure!(
        e.status == EntryStatus::Replaced
            && e.superseded_item_id == Some(a)
            && e.item_id != Some(a),
        "Replace: {e:?}"
    );
    ensure!(
        fx.get_item(a, true).is_none(),
        "the superseded id is still readable"
    );
    let r = ok(
        fx.enqueue(
            vec![NewItem::new(Some("k1"), PriorityValue::Int(3))],
            KeyConflict::Reject,
        ),
        "reject",
    )?;
    ensure!(
        r.entries[0].status == EntryStatus::Conflict,
        "Reject: {:?}",
        r.entries[0]
    );
    ok(fx.claim(1, 60_000), "claim")?;
    let r = ok(
        fx.enqueue(
            vec![NewItem::new(Some("k1"), PriorityValue::Int(3))],
            KeyConflict::ReplacePending,
        ),
        "replace leased",
    )?;
    ensure!(
        r.entries[0].status == EntryStatus::Conflict,
        "ReplacePending on a leased item: {:?}",
        r.entries[0]
    );
    Ok(())
}

fn tbl_1(fx: &Fixture) -> T {
    let put = |req: &str, policy| {
        let row: BTreeMap<FieldName, Value> = [(f("k"), Value::Int(1)), (f("req"), text(req))]
            .into_iter()
            .collect();
        fx.commit(vec![WriteOp::PutRows {
            table: o("strict"),
            rows: vec![row],
            on_conflict: policy,
        }])
    };
    ok(put("a", RowConflict::Reject), "insert")?;
    fx.advance(6_000);
    ok(put("b", RowConflict::Merge), "change")?;
    fx.advance(6_000);
    ok(put("b", RowConflict::Merge), "no-op merge")?;
    ok(put("c", RowConflict::Skip), "skip")?;
    ensure!(
        fx.get_row("strict", 1).is_some(),
        "the applied write did not reset expiry"
    );
    fx.advance(4_000);
    ensure!(
        fx.get_row("strict", 1).is_none(),
        "a no-op Merge or Skip reset expiry"
    );
    Ok(())
}

fn met_1(fx: &Fixture) -> T {
    let mut later = NewItem::new(Some("m-later"), PriorityValue::Int(1));
    later.not_before = Some(Schedule::After { delay_ms: 100_000 });
    let mut gated = NewItem::new(Some("m-gated"), PriorityValue::Int(1));
    gated.gate_keys.insert("mg".into());
    ok(
        fx.enqueue(
            vec![
                later,
                gated,
                NewItem::new(Some("m-now"), PriorityValue::Int(1)),
            ],
            KeyConflict::Duplicate,
        ),
        "enqueue",
    )?;
    block_gate(fx, "mg", GateState::Blocked)?;
    fx.advance(10);
    let m = ok(
        block_on(fx.plane.metrics(MetricsRequest {
            space: sp(),
            queue: o("work"),
        })),
        "metrics",
    )?;
    ensure!(
        m.pending == 3,
        "pending must include not-yet-due and gated items: {}",
        m.pending
    );
    ensure!(
        m.oldest_eligible_age_ms == Some(10),
        "oldest eligible age {:?}",
        m.oldest_eligible_age_ms
    );
    ok(
        fx.commit(vec![WriteOp::SetQueueState {
            queue: o("work"),
            paused: Some(true),
            drain_intake: None,
        }]),
        "pause",
    )?;
    let m = ok(
        block_on(fx.plane.metrics(MetricsRequest {
            space: sp(),
            queue: o("work"),
        })),
        "metrics paused",
    )?;
    ensure!(
        m.oldest_eligible_age_ms.is_none(),
        "a paused queue has eligible items: {:?}",
        m.oldest_eligible_age_ms
    );
    Ok(())
}
