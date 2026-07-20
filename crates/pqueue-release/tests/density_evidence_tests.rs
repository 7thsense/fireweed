use pqueue_release::density::{
    DensityMeasurement, DensityMetadata, QUEUE_ACTIVITY_DEFINITION, build_release_row,
    validate_release_row,
};

const DENSITY_KIND_HARNESS: &str = include_str!("../../../scripts/perf/tp002-e2-density-kind.sh");
const DENSITY_LOADGEN: &str = include_str!("../../pqueue-loadgen/src/main.rs");
const RESP_SERVER: &str = include_str!("../../pqueue-resp/src/lib.rs");
const SERVICE_MAIN: &str = include_str!("../../pqueue-server/src/bin/pqueue-service.rs");
const POSTGRES_WHOLE_OPERATION_ADAPTER: &str =
    include_str!("../../pqueue-server/src/postgres_native.rs");

#[test]
fn density_harness_does_not_charge_canonical_storage_to_the_memory_cap() {
    assert!(DENSITY_KIND_HARNESS.contains("emptyDir: { sizeLimit: 64Gi }"));
    assert!(!DENSITY_KIND_HARNESS.contains("emptyDir: { medium: Memory"));
    assert!(DENSITY_KIND_HARNESS.contains("limits: { cpu: \"4000m\", memory: \"4Gi\" }"));
}

#[test]
fn density_loadgen_contains_fail_closed_shape_lifecycle_and_active_load_guards() {
    assert!(DENSITY_LOADGEN.contains("assert_eq!(\n        total_queues, 1001"));
    assert!(DENSITY_LOADGEN.contains("XACK must finalize every claimed id"));
    assert!(DENSITY_LOADGEN.contains("hot_phase.store(false, Ordering::SeqCst);"));
    assert!(DENSITY_LOADGEN.contains("LifecycleIdentityLedger"));
    assert!(DENSITY_KIND_HARNESS.contains("resource_enforcement_active == true"));
    assert!(!DENSITY_KIND_HARNESS.contains("(( HOT_PHASE_RESOURCE_SAMPLES > 0 ))"));
    assert!(RESP_SERVER.contains("MAX_OBSERVED_TASKS.fetch_max(alive + 1"));
    assert!(RESP_SERVER.contains("alive_tasks >= task_limit"));
    assert!(SERVICE_MAIN.contains("set_max_runtime_tasks(64)"));
    let dispatch = POSTGRES_WHOLE_OPERATION_ADAPTER
        .split("fn dispatch")
        .nth(1)
        .expect("whole-operation dispatch exists")
        .split("match result_rx.await")
        .next()
        .unwrap();
    assert!(dispatch.contains("pqueue_resp::spawn_governed"));
    assert!(!dispatch.contains("tokio::spawn"));
}

fn measurement() -> DensityMeasurement {
    DensityMeasurement {
        hot_items: 300_000,
        control_items: 10_000,
        hot_sustain_windows: 1,
        hot_sustain_items: 300_000,
        hot_connections: 8,
        cold_worker_count: 8,
        configured_server_workers: 4,
        total_queues: 1001,
        cold_queues_active: 1000,
        cold_queues_progress_eligible: 1000,
        cold_empty_claim_responses: 0,
        hot_accepted_items: 320_000,
        hot_claimed_items: 320_000,
        hot_finalized_items: 320_000,
        cold_accepted_items: 12_000,
        cold_claimed_items: 11_000,
        cold_finalized_items: 11_000,
        cold_pending_items: 1_000,
        lost_items: 0,
        duplicate_transitions: 0,
        queue_global_progress_violations: 0,
        baseline_before_ingest_per_s: 4_900.0,
        baseline_before_claim_finalize_per_s: 4_300.0,
        baseline_after_ingest_per_s: 5_100.0,
        baseline_after_claim_finalize_per_s: 4_500.0,
        baseline_control_ingest_per_s: 4_998.0,
        baseline_control_claim_finalize_per_s: 4_397.727_272_727_273,
        hot_ingest_per_s: 4_000.0,
        hot_claim_finalize_per_s: 3_500.0,
        max_progress_latency_ms: 1_000,
        progress_bound_ms: 60_000,
        noisy_neighbor_ingest_retention_pct: 80.032_012_805_122_05,
        noisy_neighbor_claim_retention_pct: 79.586_563_307_493_54,
        shared_worker_count: 4,
        shared_worker_limit: 4,
        connection_count: 16,
        connection_limit: 32,
        task_count: 17,
        task_limit: 64,
        resource_enforcement_active: true,
        hot_phase_resource_samples: 5,
        first_hot_resource_sample_unix_ms: 1_700_000_001_000,
        last_hot_resource_sample_unix_ms: 1_700_000_059_000,
        hot_phase_started_unix_ms: 1_700_000_000_000,
        hot_phase_ended_unix_ms: 1_700_000_060_000,
    }
}

fn metadata() -> DensityMetadata {
    DensityMetadata {
        command: "scripts/perf/tp002-e2-density-kind.sh".into(),
        revision: "0123456789abcdef0123456789abcdef01234567".into(),
        topology: "live one-node kind deployment; objectlog/sqlite; 1001 generated queues".into(),
        hardware: "8 cores; 32 GiB RAM; kindest/node:v1.31.0".into(),
        seed: 42,
        duration_seconds: 60,
        queue_activity_definition: QUEUE_ACTIVITY_DEFINITION.into(),
        image_digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .into(),
        clean_revision: true,
    }
}

#[test]
fn density_validator_rejects_inconsistent_or_nonpositive_comparison() {
    for field in ["ingest", "claim"] {
        let mut measured = measurement();
        if field == "ingest" {
            measured.noisy_neighbor_ingest_retention_pct = 0.0;
        } else {
            measured.noisy_neighbor_claim_retention_pct = 100.0;
        }
        let row = build_release_row(&measured, &metadata());
        assert_eq!(row.evidence_tier, "smoke");
        assert!(validate_release_row(&row).is_err());
    }
}

#[test]
fn density_validator_rejects_incomplete_or_inconsistent_lifecycle_evidence() {
    type MeasurementMutation = Box<dyn Fn(&mut DensityMeasurement)>;
    let mutations: Vec<(&str, MeasurementMutation)> = vec![
        ("hot accepted", Box::new(|m| m.hot_accepted_items -= 1)),
        ("hot claimed", Box::new(|m| m.hot_claimed_items -= 1)),
        ("hot finalized", Box::new(|m| m.hot_finalized_items -= 1)),
        ("cold accepted", Box::new(|m| m.cold_accepted_items -= 1)),
        ("cold claimed", Box::new(|m| m.cold_claimed_items -= 1)),
        ("cold finalized", Box::new(|m| m.cold_finalized_items -= 1)),
        ("cold pending", Box::new(|m| m.cold_pending_items -= 1)),
        ("lost", Box::new(|m| m.lost_items = 1)),
        ("duplicate", Box::new(|m| m.duplicate_transitions = 1)),
        (
            "progress violation",
            Box::new(|m| m.queue_global_progress_violations = 1),
        ),
    ];

    for (name, mutate) in mutations {
        let mut measured = measurement();
        mutate(&mut measured);
        let row = build_release_row(&measured, &metadata());
        assert_eq!(row.evidence_tier, "smoke", "{name}");
        assert!(validate_release_row(&row).is_err(), "{name}");
    }
}

#[test]
fn density_validator_rejects_unbound_provenance_and_wrong_topology() {
    let mut meta = metadata();
    meta.revision = "short".into();
    meta.image_digest = "latest".into();
    meta.clean_revision = false;
    meta.command = "some-other-command".into();
    meta.topology = "in-process".into();
    meta.hardware = "unknown".into();
    let row = build_release_row(&measurement(), &meta);
    let errors = validate_release_row(&row).unwrap_err().join("\n");
    assert!(errors.contains("40-character Git SHA"));
    assert!(errors.contains("sha256 digest"));
    assert!(errors.contains("clean_revision"));
    assert!(errors.contains("command must be"));
    assert!(errors.contains("topology and hardware"));
}

#[test]
fn density_validator_rejects_self_selected_resource_limits() {
    let mut measured = measurement();
    measured.shared_worker_limit = 8;
    measured.connection_limit = 16;
    measured.task_limit = 17;
    let row = build_release_row(&measured, &metadata());
    assert_eq!(row.evidence_tier, "smoke");
    let errors = validate_release_row(&row).unwrap_err().join("\n");
    assert!(errors.contains("governed shared_worker_limit=4"));
    assert!(errors.contains("governed connection_limit=32"));
    assert!(errors.contains("governed task_limit=64"));
}

#[test]
fn density_validator_rejects_every_noncanonical_run_parameter() {
    type MeasurementMutation = Box<dyn Fn(&mut DensityMeasurement)>;
    let mutations: Vec<(&str, MeasurementMutation)> = vec![
        ("hot_items", Box::new(|m| m.hot_items = 299_999)),
        ("control_items", Box::new(|m| m.control_items = 9_999)),
        ("hot_connections", Box::new(|m| m.hot_connections = 7)),
        ("cold_worker_count", Box::new(|m| m.cold_worker_count = 7)),
        (
            "configured_server_workers",
            Box::new(|m| m.configured_server_workers = 3),
        ),
        (
            "progress_bound_ms",
            Box::new(|m| m.progress_bound_ms = 60_001),
        ),
        (
            "total_queues",
            Box::new(|m| {
                m.total_queues = 1002;
                m.cold_queues_active = 1001;
                m.cold_queues_progress_eligible = 1001;
            }),
        ),
    ];
    for (name, mutate) in mutations {
        let mut measured = measurement();
        mutate(&mut measured);
        let row = build_release_row(&measured, &metadata());
        assert_eq!(row.evidence_tier, "smoke", "{name}");
        assert!(validate_release_row(&row).is_err(), "{name}");
    }

    let mut meta = metadata();
    meta.seed = 41;
    let row = build_release_row(&measurement(), &meta);
    assert!(validate_release_row(&row).is_err());
}

#[test]
fn density_validator_requires_continuous_resource_enforcement_not_a_timed_sample() {
    let mut measured = measurement();
    measured.resource_enforcement_active = false;
    let row = build_release_row(&measured, &metadata());
    assert_eq!(row.evidence_tier, "smoke");
    assert!(validate_release_row(&row).is_err());

    let mut no_sample = measurement();
    no_sample.hot_phase_resource_samples = 0;
    no_sample.first_hot_resource_sample_unix_ms = 0;
    no_sample.last_hot_resource_sample_unix_ms = 0;
    let row = build_release_row(&no_sample, &metadata());
    validate_release_row(&row).expect("sampling cadence is not a release gate");
}

#[test]
fn density_validator_rejects_progress_semantics_substitution() {
    let mut meta = metadata();
    meta.queue_activity_definition =
        "claim completed after HOT_START, regardless of when it began".into();
    let row = build_release_row(&measurement(), &meta);
    assert_eq!(row.evidence_tier, "smoke");
    let errors = validate_release_row(&row).unwrap_err().join("\n");
    assert!(errors.contains("HOT_START claim-start semantics"));
}

#[test]
fn density_validator_rejects_quiet_host_and_absolute_speed_gates() {
    for pass_bar in [
        "run on a quiet host; throughput >= 2777 items/s",
        "p95 < 250 ms on the release machine",
    ] {
        let mut row = build_release_row(&measurement(), &metadata());
        row.pass_bar = pass_bar.into();
        let errors = validate_release_row(&row).unwrap_err().join("\n");
        assert!(errors.contains("quiet host or absolute host-speed threshold"));
    }

    let mut row = build_release_row(&measurement(), &metadata());
    row.pass_bar = "finish within 30 seconds at a rate above the operator target".into();
    let errors = validate_release_row(&row).unwrap_err().join("\n");
    assert!(errors.contains("canonical density pass bar"));

    for (key, value) in [
        ("portable_gate", serde_json::json!(false)),
        ("quiet_host_required", serde_json::json!(true)),
        ("host_speed_gate", serde_json::json!(true)),
        ("wall_clock_capacity_only", serde_json::json!(false)),
    ] {
        let mut row = build_release_row(&measurement(), &metadata());
        row.measurements.values.insert(key.into(), value);
        assert!(validate_release_row(&row).is_err(), "tampered {key}");
    }
}

#[test]
fn density_validator_rejects_direct_tampering_of_an_otherwise_valid_release_row() {
    let mutations = [
        ("hot_items", serde_json::json!(299_999)),
        ("hot_sustain_windows", serde_json::json!(0)),
        ("hot_sustain_items", serde_json::json!(299_999)),
        ("hot_connections", serde_json::json!(7)),
        ("cold_worker_count", serde_json::json!(7)),
        ("configured_server_workers", serde_json::json!(3)),
        ("total_queues", serde_json::json!(1002)),
        ("cold_queues_progress_eligible", serde_json::json!(999)),
        ("cold_empty_claim_responses", serde_json::json!(1)),
        ("progress_bound_ms", serde_json::json!(60_001)),
        ("shared_worker_count", serde_json::json!(0)),
        ("connection_count", serde_json::json!(0)),
        ("task_count", serde_json::json!(0)),
        ("resource_enforcement_active", serde_json::json!(false)),
    ];
    for (key, value) in mutations {
        let mut row = build_release_row(&measurement(), &metadata());
        assert_eq!(row.evidence_tier, "release");
        row.measurements.values.insert(key.into(), value);
        assert!(validate_release_row(&row).is_err(), "tampered {key}");
    }
}

#[test]
fn semantic_density_validator_accepts_complete_release_row() {
    let row = build_release_row(&measurement(), &metadata());
    assert_eq!(row.evidence_tier, "release");
    validate_release_row(&row).expect("complete density row passes");
}

#[test]
fn semantic_density_validator_rejects_every_required_bar_when_missing() {
    for key in [
        "total_queues",
        "baseline_before_ingest_per_s",
        "hot_ingest_per_s",
        "hot_claim_finalize_per_s",
        "cold_empty_claim_responses",
        "hot_accepted_items",
        "hot_claimed_items",
        "hot_finalized_items",
        "cold_accepted_items",
        "cold_claimed_items",
        "cold_finalized_items",
        "cold_pending_items",
        "lost_items",
        "duplicate_transitions",
        "queue_global_progress_violations",
        "shared_worker_count",
        "noisy_neighbor_ingest_retention_pct",
    ] {
        let mut row = build_release_row(&measurement(), &metadata());
        row.measurements.values.remove(key);
        assert!(
            validate_release_row(&row).is_err(),
            "missing {key} must be rejected"
        );
    }
}

#[test]
fn density_builder_never_promotes_a_failed_measurement() {
    let mut failed = measurement();
    failed.hot_ingest_per_s = 0.0;
    let row = build_release_row(&failed, &metadata());
    assert_eq!(row.evidence_tier, "smoke");
    assert_eq!(row.scale, "smoke");
    assert!(validate_release_row(&row).is_err());
}
