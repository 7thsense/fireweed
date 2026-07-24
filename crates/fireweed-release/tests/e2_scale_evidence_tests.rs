use fireweed_release::e2::{
    E2ScalePoint, E2Tuning, build_e2_row, evaluate_e2_bars, expected_one_owner_confirmations,
    validate_release_rows,
};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

fn points() -> Vec<E2ScalePoint> {
    [2, 4, 8]
        .into_iter()
        .map(|owners| E2ScalePoint {
            owners,
            ingest_aggregate: owners as f64 * 3_200.0,
            ingest_min_per_queue: 3_000.0,
            drain_aggregate: owners as f64 * 25_000.0,
            drain_min_per_queue: 24_000.0,
            one_owner_confirmations: expected_one_owner_confirmations(owners, 1),
            queues_per_owner: 1,
            items_per_queue: 12_000,
            conns_per_queue: 8,
        })
        .collect()
}

fn rows() -> Vec<fireweed_release::LedgerRow> {
    let points = points();
    let verdict = evaluate_e2_bars(&points);
    (1..=3)
        .map(|sweep| {
            build_e2_row(
                &points,
                &E2Tuning {
                    source_revision: REVISION.into(),
                    segment_max_latency_ms: 1,
                    segment_target_bytes: 262_144,
                    worker_threads_per_node: 2,
                    server_cpu_limit: "1300m".into(),
                    server_cpu_request: "1000m".into(),
                    loadgen_cpu_limit: "2000m".into(),
                    cores: 12,
                    kind_node_image: "kindest/node:v1.36.1@sha256:exact".into(),
                    pipe_size: 1_000,
                    batch_size: 1_000,
                    sweep,
                },
                &verdict,
            )
        })
        .collect()
}

#[test]
fn exact_three_sweep_authority_passes() {
    validate_release_rows(&rows(), REVISION).expect("canonical three-sweep authority");
}

#[test]
fn missing_and_extra_rows_are_rejected() {
    let mut missing = rows();
    missing.pop();
    assert!(
        validate_release_rows(&missing, REVISION)
            .unwrap_err()
            .iter()
            .any(|error| error.contains("exactly 3 rows"))
    );

    let mut extra = rows();
    extra.push(extra[0].clone());
    assert!(
        validate_release_rows(&extra, REVISION)
            .unwrap_err()
            .iter()
            .any(|error| error.contains("exactly 3 rows"))
    );
}

#[test]
fn duplicate_or_noncanonical_sweeps_are_rejected() {
    let mut duplicate = rows();
    duplicate[2]
        .measurements
        .values
        .insert("sweep".into(), serde_json::json!(2));
    let errors = validate_release_rows(&duplicate, REVISION)
        .unwrap_err()
        .join("\n");
    assert!(errors.contains("duplicates sweep 2"));
    assert!(errors.contains("unique sweeps"));

    let mut noncanonical = rows();
    noncanonical[2]
        .measurements
        .values
        .insert("sweep".into(), serde_json::json!(4));
    assert!(
        validate_release_rows(&noncanonical, REVISION)
            .unwrap_err()
            .iter()
            .any(|error| error.contains("non-canonical sweep 4"))
    );
}

#[test]
fn mixed_revision_configuration_topology_and_seed_are_rejected() {
    let mut changed = rows();
    changed[1].measurements.values.insert(
        "source_revision".into(),
        serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    );
    changed[2]
        .measurements
        .values
        .insert("segment_target_bytes".into(), serde_json::json!(131_072));
    changed[2].environment.push_str(" altered");
    changed[2].seed = 9;
    let errors = validate_release_rows(&changed, REVISION)
        .unwrap_err()
        .join("\n");
    assert!(errors.contains("source_revision does not match expected revision"));
    assert!(errors.contains("segment_target_bytes must equal canonical value 262144"));
    assert!(errors.contains("stable producer topology or identity fields"));
    assert!(errors.contains("seed must be the canonical value 0"));
}

#[test]
fn canonical_shape_fields_cannot_be_relabeled() {
    for (key, value) in [
        ("queues_per_owner", serde_json::json!(2)),
        ("items_per_queue", serde_json::json!(11_999)),
        ("conns_per_queue", serde_json::json!(7)),
        ("pipe_size", serde_json::json!(999)),
        ("batch_size", serde_json::json!(999)),
        ("worker_threads_per_node", serde_json::json!(3)),
    ] {
        let mut changed = rows();
        changed[1].measurements.values.insert(key.into(), value);
        assert!(
            validate_release_rows(&changed, REVISION)
                .unwrap_err()
                .iter()
                .any(|error| error.contains(key)),
            "{key}"
        );
    }
}
