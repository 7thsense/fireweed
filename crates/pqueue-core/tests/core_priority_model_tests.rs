// B-011: priority_sort encoding — AC-CORE-1
//
// Verifies that `priority_sort` produces a byte sequence whose lexicographic
// order matches the declared total order for all four priority models in both
// directions, over ≥ 1,000,000 generated pairs.

use proptest::prelude::*;
use pqueue_core::{
    DecimalValue, PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker,
    PriorityValue, UtcTimestamp, priority_sort,
};
use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// Reference comparison functions (ground truth for the test oracle)
// ---------------------------------------------------------------------------

fn cmp_timestamp(a: &UtcTimestamp, b: &UtcTimestamp) -> Ordering {
    a.seconds.cmp(&b.seconds).then(a.nanoseconds.cmp(&b.nanoseconds))
}

fn cmp_int64(a: i64, b: i64) -> Ordering {
    a.cmp(&b)
}

/// Compare two decimal values as rationals without intermediate float conversion.
///
/// a = ma × 10^(-sa), b = mb × 10^(-sb)
/// a < b ↔ ma × 10^sb < mb × 10^sa (cross-multiply, adjusting for sign)
fn cmp_decimal(a: &DecimalValue, b: &DecimalValue) -> Ordering {
    // Handle sign cases first.
    let sign_a = a.mantissa.signum();
    let sign_b = b.mantissa.signum();
    match sign_a.cmp(&sign_b) {
        Ordering::Less => return Ordering::Less,
        Ordering::Greater => return Ordering::Greater,
        Ordering::Equal => {}
    }
    if sign_a == 0 {
        return Ordering::Equal;
    }

    // Same sign. Compare absolute values, then apply sign.
    let abs_a = a.mantissa.unsigned_abs();
    let abs_b = b.mantissa.unsigned_abs();

    // Cross-multiply: abs_a × 10^sb vs abs_b × 10^sa
    // To avoid overflow, keep scale differences small (generators constrain this).
    let abs_ord = if a.scale == b.scale {
        abs_a.cmp(&abs_b)
    } else if a.scale < b.scale {
        // Multiply abs_a by 10^(sb-sa) to equalize scale.
        let diff = b.scale - a.scale;
        if diff <= 38 {
            let scaled_a = abs_a.saturating_mul(10u128.pow(diff));
            scaled_a.cmp(&abs_b)
        } else {
            Ordering::Greater // abs_a × 10^large > abs_b unless abs_a == 0
        }
    } else {
        let diff = a.scale - b.scale;
        if diff <= 38 {
            let scaled_b = abs_b.saturating_mul(10u128.pow(diff));
            abs_a.cmp(&scaled_b)
        } else {
            Ordering::Less
        }
    };

    // For negatives, invert the absolute ordering.
    if sign_a < 0 { abs_ord.reverse() } else { abs_ord }
}

fn cmp_text(a: &str, b: &str) -> Ordering {
    a.cmp(b)
}

fn reference_cmp(a: &PriorityValue, b: &PriorityValue) -> Ordering {
    match (a, b) {
        (PriorityValue::Timestamp(ta), PriorityValue::Timestamp(tb)) => cmp_timestamp(ta, tb),
        (PriorityValue::Int64(ia), PriorityValue::Int64(ib)) => cmp_int64(*ia, *ib),
        (PriorityValue::Decimal(da), PriorityValue::Decimal(db)) => cmp_decimal(da, db),
        (PriorityValue::Text(sa), PriorityValue::Text(sb)) => cmp_text(sa, sb),
        _ => panic!("mixed-kind comparison"),
    }
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

fn arb_timestamp() -> impl Strategy<Value = PriorityValue> {
    (i64::MIN..i64::MAX, 0u32..1_000_000_000u32).prop_map(|(s, ns)| {
        PriorityValue::Timestamp(UtcTimestamp::new(s, ns).unwrap())
    })
}

fn arb_int64() -> impl Strategy<Value = PriorityValue> {
    any::<i64>().prop_map(PriorityValue::Int64)
}

fn arb_decimal() -> impl Strategy<Value = PriorityValue> {
    // Constrain to avoid overflow in the reference cross-multiply: scale ≤ 18, mantissa small.
    (-10_000_000_000i128..10_000_000_000i128, 0u32..10u32)
        .prop_map(|(m, s)| PriorityValue::Decimal(DecimalValue { mantissa: m, scale: s }))
}

fn arb_text() -> impl Strategy<Value = PriorityValue> {
    "[a-z]{0,16}".prop_map(PriorityValue::Text)
}

fn model_for(kind: PriorityModelKind, direction: PriorityDirection) -> PriorityModel {
    PriorityModel {
        kind,
        direction,
        tie_breaker: PriorityTieBreaker::CreatedSequence,
    }
}

// ---------------------------------------------------------------------------
// Property: byte order == declared total order
// ---------------------------------------------------------------------------

fn assert_order_preserved(a: &PriorityValue, b: &PriorityValue, model: &PriorityModel) {
    let ref_ord = reference_cmp(a, b);
    let enc_a = priority_sort(a, model);
    let enc_b = priority_sort(b, model);
    let enc_ord = enc_a.cmp(&enc_b);

    let expected = match model.direction {
        PriorityDirection::Ascending => ref_ord,
        PriorityDirection::Descending => ref_ord.reverse(),
    };

    assert_eq!(
        enc_ord, expected,
        "encoding order mismatch: ref={:?}, enc={:?}, a={:?}, b={:?}, model={:?}",
        ref_ord, enc_ord, a, b, model
    );
}

// ---------------------------------------------------------------------------
// Proptest suite: AC-CORE-1
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 250_000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn core_priority_model_tests_timestamp_ascending(
        a in arb_timestamp(), b in arb_timestamp()
    ) {
        let model = model_for(PriorityModelKind::Timestamp, PriorityDirection::Ascending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_timestamp_descending(
        a in arb_timestamp(), b in arb_timestamp()
    ) {
        let model = model_for(PriorityModelKind::Timestamp, PriorityDirection::Descending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_int64_ascending(
        a in arb_int64(), b in arb_int64()
    ) {
        let model = model_for(PriorityModelKind::Int64, PriorityDirection::Ascending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_int64_descending(
        a in arb_int64(), b in arb_int64()
    ) {
        let model = model_for(PriorityModelKind::Int64, PriorityDirection::Descending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_decimal_ascending(
        a in arb_decimal(), b in arb_decimal()
    ) {
        let model = model_for(PriorityModelKind::Decimal, PriorityDirection::Ascending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_decimal_descending(
        a in arb_decimal(), b in arb_decimal()
    ) {
        let model = model_for(PriorityModelKind::Decimal, PriorityDirection::Descending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_text_ascending(
        a in arb_text(), b in arb_text()
    ) {
        let model = model_for(PriorityModelKind::Text, PriorityDirection::Ascending);
        assert_order_preserved(&a, &b, &model);
    }

    #[test]
    fn core_priority_model_tests_text_descending(
        a in arb_text(), b in arb_text()
    ) {
        let model = model_for(PriorityModelKind::Text, PriorityDirection::Descending);
        assert_order_preserved(&a, &b, &model);
    }
}

// ---------------------------------------------------------------------------
// Spot-check: known boundary values
// ---------------------------------------------------------------------------

#[test]
fn core_priority_model_tests_int64_boundaries() {
    let asc = model_for(PriorityModelKind::Int64, PriorityDirection::Ascending);
    let min = PriorityValue::Int64(i64::MIN);
    let neg = PriorityValue::Int64(-1);
    let zero = PriorityValue::Int64(0);
    let pos = PriorityValue::Int64(1);
    let max = PriorityValue::Int64(i64::MAX);

    let mut sorted = vec![&max, &zero, &min, &pos, &neg];
    sorted.sort_by_key(|a| priority_sort(a, &asc));

    assert_eq!(sorted, vec![&min, &neg, &zero, &pos, &max]);
}

#[test]
fn core_priority_model_tests_decimal_equivalent_representations() {
    let model = model_for(PriorityModelKind::Decimal, PriorityDirection::Ascending);
    // 1.0 expressed three ways must all encode identically.
    let a = PriorityValue::Decimal(DecimalValue { mantissa: 1, scale: 0 });
    let b = PriorityValue::Decimal(DecimalValue { mantissa: 10, scale: 1 });
    let c = PriorityValue::Decimal(DecimalValue { mantissa: 100, scale: 2 });

    assert_eq!(priority_sort(&a, &model), priority_sort(&b, &model));
    assert_eq!(priority_sort(&b, &model), priority_sort(&c, &model));
}

#[test]
fn core_priority_model_tests_text_empty_sorts_first() {
    let model = model_for(PriorityModelKind::Text, PriorityDirection::Ascending);
    let empty = PriorityValue::Text(String::new());
    let a = PriorityValue::Text("a".to_string());
    assert!(priority_sort(&empty, &model) < priority_sort(&a, &model));
}
