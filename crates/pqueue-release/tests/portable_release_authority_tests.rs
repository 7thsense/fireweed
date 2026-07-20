use std::collections::BTreeMap;

use pqueue_release::{LedgerRow, Measurements};

const REV: &str = "0123456789abcdef0123456789abcdef01234567";

fn portable(id: &str) -> LedgerRow {
    let mut values = BTreeMap::from([
        ("bars_met".into(), serde_json::json!(true)),
        ("portable_gate".into(), serde_json::json!(true)),
        ("quiet_host_required".into(), serde_json::json!(false)),
        ("host_speed_gate".into(), serde_json::json!(false)),
        ("wall_clock_capacity_only".into(), serde_json::json!(true)),
        ("exact_outcomes".into(), serde_json::json!(true)),
        ("monotonic_progress".into(), serde_json::json!(true)),
        ("bounded_resources".into(), serde_json::json!(true)),
        ("source_revision".into(), serde_json::json!(REV)),
        ("resident_backlog".into(), serde_json::json!(10_000_000)),
        ("lost_items".into(), serde_json::json!(0)),
        ("duplicate_claims".into(), serde_json::json!(0)),
    ]);
    if id == "E0" {
        values.extend([
            ("accepted_items".into(), serde_json::json!(10_000_000)),
            ("claimed_items".into(), serde_json::json!(10_000_000)),
            ("finalized_items".into(), serde_json::json!(10_000_000)),
        ]);
    } else {
        values.insert("batch_sizes".into(), serde_json::json!([1, 100, 1000]));
    }
    LedgerRow {
        suite: "performance_single_deployment_baseline_tests".into(),
        command: "exact producer".into(),
        backend_profile: "postgres_native".into(),
        scale: "release".into(),
        seed: 0,
        environment: "declared topology under ordinary load".into(),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "exact outcomes progress and bounded resources; timings are capacity only".into(),
        evidence_tier: "release".into(),
        measurements: Measurements {
            tp002_evidence_ids: vec![id.into()],
            values,
        },
    }
}

#[test]
fn portable_e0_e1_accept_semantics_not_machine_speed() {
    for id in ["E0", "E1"] {
        pqueue_release::single_deployment::validate_row(&portable(id), id, REV).unwrap();
    }
}

#[test]
fn e0_e1_reject_quiet_host_and_absolute_speed_bars() {
    for text in ["quiet host required", "throughput >= 2778", "p99 < 1000ms"] {
        let mut row = portable("E0");
        row.pass_bar = text.into();
        assert!(pqueue_release::single_deployment::validate_row(&row, "E0", REV).is_err());
    }
}
