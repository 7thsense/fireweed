use pqueue_release::density::{
    DensityMeasurement, DensityMetadata, build_release_row, validate_release_row,
};

fn measurement() -> DensityMeasurement {
    DensityMeasurement {
        total_queues: 1001,
        cold_queues_active: 1000,
        cold_queues_progress_eligible: 1000,
        hot_ingest_per_s: 4_000.0,
        hot_claim_finalize_per_s: 3_500.0,
        progress_bound_violations: 0,
        max_progress_latency_ms: 1_000,
        progress_bound_ms: 60_000,
        noisy_neighbor_ingest_retention_pct: 102.5,
        noisy_neighbor_claim_retention_pct: 101.0,
        shared_worker_count: 8,
        shared_worker_limit: 64,
        connection_count: 16,
        connection_limit: 32,
        task_count: 17,
        task_limit: 256,
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
        queue_activity_definition: "cold queue completed claim/finalize and retained one eligible item while the hot queue ran".into(),
        image_digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        clean_revision: true,
    }
}

#[test]
fn density_validator_rejects_retention_below_no_degradation_bar() {
    for field in ["ingest", "claim"] {
        let mut measured = measurement();
        if field == "ingest" {
            measured.noisy_neighbor_ingest_retention_pct = 99.99;
        } else {
            measured.noisy_neighbor_claim_retention_pct = 99.99;
        }
        let row = build_release_row(&measured, &metadata());
        assert_eq!(row.evidence_tier, "smoke");
        assert!(validate_release_row(&row).is_err());
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
    measured.shared_worker_limit = measured.shared_worker_count;
    measured.connection_limit = measured.connection_count;
    measured.task_limit = measured.task_count;
    let row = build_release_row(&measured, &metadata());
    assert_eq!(row.evidence_tier, "smoke");
    let errors = validate_release_row(&row).unwrap_err().join("\n");
    assert!(errors.contains("governed shared_worker_limit=64"));
    assert!(errors.contains("governed connection_limit=32"));
    assert!(errors.contains("governed task_limit=256"));
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
        "hot_ingest_per_s",
        "hot_claim_finalize_per_s",
        "progress_bound_violations",
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
    failed.hot_ingest_per_s = 1.0;
    let row = build_release_row(&failed, &metadata());
    assert_eq!(row.evidence_tier, "smoke");
    assert_eq!(row.scale, "smoke");
    assert!(validate_release_row(&row).is_err());
}
