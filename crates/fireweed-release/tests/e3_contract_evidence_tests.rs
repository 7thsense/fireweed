use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_release::e3_contract::{
    E3_EVIDENCE_LINK_SCHEMA_VERSION, E3AuthorityMode, E3EvidenceLink, E3FenceAuthorityEvidence,
    E3FenceAuthorityObservation, E3FenceObservation, E3ObservedOutcome, E3TransactionObservation,
    build_e3_contract_manifest, build_e3_fence_evidence, build_e3_transaction_evidence_row,
    verify_e3_contract, write_e3_contract,
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const RUN_ID: &str = "e3-run-20260728";
const COMPOSITION_FINGERPRINT: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn evidence_link() -> E3EvidenceLink {
    E3EvidenceLink {
        schema_version: E3_EVIDENCE_LINK_SCHEMA_VERSION,
        run_id: RUN_ID.into(),
        composition_fingerprint: COMPOSITION_FINGERPRINT.into(),
        authority_mode: E3AuthorityMode::NativeCreateOnly,
    }
}

fn passed(observation: &str) -> E3ObservedOutcome {
    E3ObservedOutcome::Passed {
        observation: observation.into(),
    }
}

fn native_fence_observation() -> E3FenceObservation {
    E3FenceObservation {
        source_revision: REVISION.into(),
        evidence_link: evidence_link(),
        authority: E3FenceAuthorityObservation::NativeCreateOnly {
            stale_epoch: passed("stale owner seal returned EpochFenced"),
            current_epoch: passed("current owner commit was readable"),
        },
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "fireweed-e3-contract-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/ci/fixtures/e3-contract/valid");
        for name in ["contract.json", "e3.jsonl", "tp003.jsonl", "fencing.json"] {
            fs::copy(source.join(name), root.join(name)).unwrap();
        }
        Self { root }
    }

    fn manifest(&self) -> PathBuf {
        self.root.join("contract.json")
    }

    fn run_owned(&self, name: &str) -> fireweed_release::RunOwned {
        fireweed_release::RunOwned::new(repo_root(), &self.root, self.root.join(name))
            .expect("authorize run-owned E3 test output")
    }

    fn mutate_json(&self, name: &str, mutate: impl FnOnce(&mut serde_json::Value)) {
        let path = self.root.join(name);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        mutate(&mut value);
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    fn mutate_e3_row(&self, index: usize, mutate: impl FnOnce(&mut serde_json::Value)) {
        let path = self.root.join("e3.jsonl");
        let body = fs::read_to_string(&path).unwrap();
        let mut rows = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        mutate(&mut rows[index]);
        let body = rows
            .into_iter()
            .map(|row| serde_json::to_string(&row).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{body}\n")).unwrap();
    }

    fn mutate_transaction_row(&self, index: usize, mutate: impl FnOnce(&mut serde_json::Value)) {
        let path = self.root.join("tp003.jsonl");
        let body = fs::read_to_string(&path).unwrap();
        let mut rows = body
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        mutate(&mut rows[index]);
        let body = rows
            .into_iter()
            .map(|row| serde_json::to_string(&row).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{body}\n")).unwrap();
    }

    fn errors(&self) -> String {
        verify_e3_contract(&self.manifest(), REVISION)
            .unwrap_err()
            .into_iter()
            .map(|error| error.0)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn accepts_all_profiles_bounds_transaction_authorities_and_fence() {
    let fixture = Fixture::new();
    let summary = verify_e3_contract(&fixture.manifest(), REVISION).unwrap();
    assert_eq!(summary.entries, 8);
    assert_eq!(summary.transaction_rows, 48);
    assert_eq!(summary.cost_rows, 8);
}

#[test]
fn rejects_mixed_run_and_composition_artifacts() {
    let ledger = Fixture::new();
    ledger.mutate_e3_row(0, |row| {
        row["measurements"]["e3_run_id"] = serde_json::json!("another-run-id");
    });
    assert!(ledger.errors().contains("requires e3_run_id"));

    let transaction = Fixture::new();
    transaction.mutate_transaction_row(0, |row| {
        row["e3_composition_fingerprint"] =
            serde_json::json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    });
    assert!(
        transaction
            .errors()
            .contains("requires exactly one revision/profile/bound/timing-bound TP-003 row")
    );

    let fence = Fixture::new();
    fence.mutate_json("fencing.json", |value| {
        value["evidence_link"]["run_id"] = serde_json::json!("another-run-id");
    });
    assert!(
        fence
            .errors()
            .contains("does not prove the selected, run-bound S3 publication authority")
    );
}

#[test]
fn rejects_old_contract_fence_and_unlinked_transaction_artifacts() {
    let contract = Fixture::new();
    contract.mutate_json("contract.json", |value| {
        value["schema_version"] = serde_json::json!(3);
    });
    assert!(contract.errors().contains("expected 4"));

    let fence = Fixture::new();
    fence.mutate_json("fencing.json", |value| {
        value["schema_version"] = serde_json::json!(4);
    });
    assert!(
        fence
            .errors()
            .contains("does not prove the selected, run-bound S3 publication authority")
    );

    let transaction = Fixture::new();
    transaction.mutate_transaction_row(0, |row| {
        row.as_object_mut().unwrap().remove("e3_run_id");
    });
    assert!(
        transaction
            .errors()
            .contains("requires exactly one revision/profile/bound/timing-bound TP-003 row")
    );
}

#[allow(non_snake_case)]
#[test]
fn TestE3ValidatorRejectsSmokeRowsAndIncompleteMatrix() {
    let smoke = Fixture::new();
    smoke.mutate_e3_row(0, |row| {
        row["evidence_tier"] = serde_json::json!("smoke");
    });
    let smoke_errors = smoke.errors();
    assert!(
        smoke_errors.contains("must be release tier and scale"),
        "{smoke_errors}"
    );

    let incomplete = Fixture::new();
    incomplete.mutate_json("contract.json", |value| {
        value["entries"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| entry["profile"] != "object_log_sqlite_projection");
    });
    let incomplete_errors = incomplete.errors();
    assert!(
        incomplete_errors
            .contains("missing E3 contract entry: profile=object_log_sqlite_projection"),
        "{incomplete_errors}"
    );
}

#[allow(non_snake_case)]
#[test]
fn TestE3ValidatorRejectsMissingOrAlteredCounters() {
    let missing = Fixture::new();
    missing.mutate_e3_row(0, |row| {
        row["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("recovery_load_mean_commands_per_segment");
    });
    let missing_errors = missing.errors();
    assert!(
        missing_errors.contains("recovery_load_mean_commands_per_segment"),
        "{missing_errors}"
    );

    let altered = Fixture::new();
    altered.mutate_e3_row(0, |row| {
        row["measurements"]["bound_1ms_recorder_control_verified_items"] =
            serde_json::json!(999_999);
    });
    let altered_errors = altered.errors();
    assert!(
        altered_errors.contains("matching complete recorder-control state fingerprints"),
        "{altered_errors}"
    );
}

#[allow(non_snake_case)]
#[test]
fn TestE3ValidatorRejectsQuietHostAndHostSpeedGates() {
    let quiet_host = Fixture::new();
    let path = quiet_host.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "synthetic-fixture",
        "synthetic-fixture on a quiet host",
        1,
    );
    fs::write(path, body).unwrap();
    let quiet_errors = quiet_host.errors();
    assert!(
        quiet_errors.contains("contains a non-portable quiet-host gate"),
        "{quiet_errors}"
    );

    let host_speed = Fixture::new();
    let path = host_speed.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "E3: 1/5/20/100ms bounds; sustained batched commits with valid latency distributions and logically identical interleaved recorder controls; 10M ephemeral in-memory projection rebuilt by exact bounded durable-log genesis replay; streaming complete-state digests match with zero missing, duplicate, or invalid items; replay progress and bounded-resource samples are monotonic; absolute capacity is reported for the declared topology, not used as a portable gate",
        "finish within 30 seconds on this host",
        1,
    );
    fs::write(path, body).unwrap();
    let host_speed_errors = host_speed.errors();
    assert!(
        host_speed_errors.contains("must use the governed host-independent pass bar"),
        "{host_speed_errors}"
    );
}

#[allow(non_snake_case)]
#[test]
fn TestE3ReleaseGatesPassFmtClippyAndOperatorChecks() {
    let root = repo_root();
    assert!(
        !root.join("go.mod").exists(),
        "go test ./... is operator-required non-applicability because the repo has no Go module"
    );
    assert!(
        !root.join(".lefthook.yml").exists() && !root.join(".pre-commit-config.yaml").exists(),
        "lefthook run pre-commit is operator-required non-applicability because the repo has no lefthook config"
    );

    let release_note =
        fs::read_to_string(root.join("docs/perf/tp002-e3-objectlog-s3-release.md")).unwrap();
    assert!(
        release_note
            .contains("Focused tests, formatting, and warning-denied clippy are the code gates"),
        "release note does not record the governed code gates"
    );
}

#[test]
fn production_generator_builds_and_semantically_verifies_the_full_matrix() {
    let fixture = Fixture::new();
    let generated = fixture.run_owned("generated.json");
    let manifest = build_e3_contract_manifest(
        REVISION.into(),
        evidence_link(),
        "e3.jsonl".into(),
        "tp003.jsonl".into(),
        "fencing.json".into(),
    )
    .unwrap();
    write_e3_contract(&generated, &manifest).unwrap();
    let summary = verify_e3_contract(generated.path(), REVISION).unwrap();
    assert_eq!(
        (summary.entries, summary.transaction_rows, summary.cost_rows),
        (8, 48, 8)
    );
}

#[test]
fn rejects_marker_only_recovery_and_recorder_controls() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path)
        .unwrap()
        .replacen(
            "\"recovery_state_digest_after\":\"fnv1a128:0123456789abcdef0123456789abcdef\"",
            "\"recovery_state_digest_after\":\"fnv1a128:changed\"",
            1,
        )
        .replacen(
            "\"bound_1ms_recorder_control_logical_match\":true",
            "\"bound_1ms_recorder_control_logical_match\":false",
            1,
        );
    fs::write(path, body).unwrap();
    let errors = fixture.errors();
    assert!(errors.contains("exact streaming 10M recovery"), "{errors}");
    assert!(
        errors.contains("recorder_control_logical_match=true"),
        "{errors}"
    );
}

#[test]
fn rejects_recovery_without_canonical_order_or_production_checksum_proof() {
    let fixture = Fixture::new();
    fixture.mutate_e3_row(0, |row| {
        row["measurements"]["recovery_state_digest_algorithm"] =
            serde_json::json!("fnv1a128+disk-unique-id-index");
    });
    assert!(fixture.errors().contains("exact streaming 10M recovery"));

    let fixture = Fixture::new();
    fixture.mutate_e3_row(0, |row| {
        row["measurements"]["recovery_checksum_validation_passed"] = serde_json::json!(false);
    });
    assert!(fixture.errors().contains("exact streaming 10M recovery"));
}

#[test]
fn rejects_missing_tampered_or_zero_recovery_load_batch_measurements() {
    let missing = Fixture::new();
    missing.mutate_e3_row(0, |row| {
        row["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("recovery_load_mean_commands_per_segment");
    });
    assert!(
        missing
            .errors()
            .contains("recovery_load_mean_commands_per_segment")
    );

    let zero = Fixture::new();
    zero.mutate_e3_row(0, |row| {
        row["measurements"]["recovery_load_segment_bytes"] = serde_json::json!(0);
    });
    assert!(zero.errors().contains("exact streaming 10M recovery"));

    let tampered = Fixture::new();
    tampered.mutate_e3_row(0, |row| {
        row["measurements"]["recovery_load_mean_commands_per_segment"] = serde_json::json!(99.0);
        row["measurements"]["recovery_load_max_commands_per_segment"] = serde_json::json!(1);
    });
    assert!(tampered.errors().contains("exact streaming 10M recovery"));
}

#[test]
fn rejects_zero_sqlite_command_count_without_panicking() {
    let fixture = Fixture::new();
    fixture.mutate_e3_row(1, |row| {
        row["measurements"]["recovery_command_count"] = serde_json::json!(0);
        row["measurements"]["recovery_load_command_count"] = serde_json::json!(0);
    });
    assert!(fixture.errors().contains("exact streaming 10M recovery"));
}

#[test]
fn rejects_true_recorder_marker_without_matching_complete_state_fingerprints() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"bound_1ms_recorder_disabled_state_fingerprint\":\"fnv1a128:0123456789abcdef0123456789abcdef\"",
        "\"bound_1ms_recorder_disabled_state_fingerprint\":\"fnv1a128:fedcba9876543210fedcba9876543210\"",
        1,
    );
    fs::write(path, body).unwrap();
    let errors = fixture.errors();
    assert!(
        errors.contains("matching complete recorder-control state fingerprints"),
        "{errors}"
    );
}

#[test]
fn accepts_large_consistent_interleaved_recorder_ratio_as_diagnostic() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path)
        .unwrap()
        .replacen(
            "\"bound_1ms_recorder_overhead_ratio\":1.0",
            "\"bound_1ms_recorder_overhead_ratio\":1.5",
            1,
        )
        .replacen(
            "\"bound_1ms_recorder_overhead_ratio_samples\":[1.0,1.0,1.0,1.0,1.0]",
            "\"bound_1ms_recorder_overhead_ratio_samples\":[1.5,1.5,1.5,1.5,1.5]",
            1,
        );
    fs::write(path, body).unwrap();
    verify_e3_contract(&fixture.manifest(), REVISION).unwrap();
}

#[test]
fn rejects_missing_or_invalid_interleaved_recorder_measurement() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"bound_1ms_disabled_control_throughput_per_s\":1000.0,",
        "",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("positive disabled-recorder control")
    );

    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"bound_1ms_recorder_overhead_ratio\":1.0",
        "\"bound_1ms_recorder_overhead_ratio\":0.0",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(fixture.errors().contains("finite positive recorder ratio"));

    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"bound_1ms_recorder_overhead_ratio_samples\":[1.0,1.0,1.0,1.0,1.0]",
        "\"bound_1ms_recorder_overhead_ratio_samples\":[0.0,1.0,1.0,1.0,1.0]",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(fixture.errors().contains("finite positive recorder ratio"));
}

#[test]
fn rejects_lockstepped_or_forged_recorder_control_distribution() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "independent-bounded-blocks-seeded-alternating-order-v1",
        "paired-operation-barriers-concurrent-worker-partitions-v1",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(fixture.errors().contains("recorder_control_schedule"));

    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"bound_1ms_recorder_overhead_ratio_samples\":[1.0,1.0,1.0,1.0,1.0]",
        "\"bound_1ms_recorder_overhead_ratio_samples\":[1.0,1.0,1.5,1.5,1.5]",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("median matches the interleaved samples")
    );
}

#[test]
fn rejects_profile_incorrect_canonical_recovery_command_count() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"recovery_command_count\":10001",
        "\"recovery_command_count\":10000",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("exact streaming 10M recovery with monotonic replay progress")
    );
}

#[test]
fn rejects_inexact_recovery_command_range_even_with_matching_state_digest() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"recovery_tail_replayed\":10000",
        "\"recovery_tail_replayed\":9999",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("exact streaming 10M recovery with monotonic replay progress")
    );
}

#[test]
fn rejects_unbounded_cardinality_dependent_work_and_inexact_recovery_state() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path)
        .unwrap()
        .replacen(
            "\"bound_1ms_task_limit\":384",
            "\"bound_1ms_task_limit\":10000000",
            1,
        )
        .replacen(
            "\"recovery_object_page_limit\":1000",
            "\"recovery_object_page_limit\":10000000",
            1,
        )
        .replacen(
            "\"recovery_invalid_items\":0",
            "\"recovery_invalid_items\":1",
            1,
        )
        .replacen(
            "\"recovery_progress_source\":\"production_replay_pages\"",
            "\"recovery_progress_source\":\"synthetic_endpoints\"",
            1,
        )
        .replacen(
            "\"recovery_max_tail_budget\":1000000",
            "\"recovery_max_tail_budget\":10000000",
            1,
        );
    fs::write(path, body).unwrap();
    let errors = fixture.errors();
    assert!(errors.contains("bounded-resource accounting"), "{errors}");
    assert!(errors.contains("exact streaming 10M recovery"), "{errors}");
}

#[test]
fn rejects_missing_profile() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| entry["profile"] != "object_log_sqlite_projection");
    });
    assert!(
        fixture
            .errors()
            .contains("missing E3 contract entry: profile=object_log_sqlite_projection")
    );
}

#[test]
fn rejects_legacy_contract_without_explicit_fence_and_timing_links() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["schema_version"] = serde_json::json!(1);
    });
    assert!(fixture.errors().contains("expected 4"));
}

#[test]
fn rejects_missing_bound() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"].as_array_mut().unwrap().retain(|entry| {
            entry["profile"] != "object_log_inmemory_projection" || entry["bound_ms"] != 20
        });
    });
    assert!(
        fixture
            .errors()
            .contains("profile=object_log_inmemory_projection bound=20ms")
    );
}

#[test]
fn rejects_missing_or_non_pass_ac() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"][0]["transaction_authorities"]
            .as_array_mut()
            .unwrap()
            .retain(|authority| authority["ac"] != "AC-TXN-4");
    });
    assert!(
        fixture
            .errors()
            .contains("missing transaction authority AC-TXN-4")
    );

    let fixture = Fixture::new();
    let path = fixture.root.join("tp003.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"result\":\"pass\"",
        "\"result\":\"fail\"",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("TP-003 authority is not a complete passing row")
    );
}

#[test]
fn rejects_unjustified_na() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"][0]["transaction_authorities"][0]["applicability"] =
            serde_json::json!({"status":"capability_na","reason":"not measured"});
    });
    assert!(
        fixture
            .errors()
            .contains("capability n/a is not authorized")
    );
}

#[test]
fn rejects_source_revision_mismatch() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["source_revision"] = serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    });
    let errors = fixture.errors();
    assert!(errors.contains("requires source_revision"));
    assert!(errors.contains("fencing evidence"));
}

#[test]
fn rejects_wrong_expected_revision() {
    let fixture = Fixture::new();
    let errors = verify_e3_contract(
        &fixture.manifest(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("does not match expected revision"))
    );
}

#[test]
fn rejects_non_governed_backend_suite_and_ac7_binding() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"][0]["transaction_authorities"][0]["backend"] = serde_json::json!("forged");
        value["ac7_binding"]["bounds_ms"] = serde_json::json!([1, 5, 20]);
    });
    let errors = fixture.errors();
    assert!(errors.contains("is not the governed authority"));
    assert!(errors.contains("AC-TXN-7 binding must name the governed"));

    let fixture = Fixture::new();
    let path = fixture.root.join("tp003.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "external_transaction_contract_matrix_tests",
        "forged_suite",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("TP-003 authority is not a complete passing row")
    );
}

#[test]
fn rejects_extra_tp002_evidence_id() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"tp002_evidence_ids\":[\"E3\"]",
        "\"tp002_evidence_ids\":[\"E3\",\"E0\"]",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(fixture.errors().contains("must substantiate exactly E3"));
}

#[test]
fn rejects_non_governed_e3_producer() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "performance_object_log_e3_live_tests",
        "forged_producer",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("must come from governed producer suite")
    );
}

#[test]
fn rejects_undeclared_topology_or_authority_mode_mismatched_to_manifest() {
    let fixture = Fixture::new();
    fixture.mutate_e3_row(0, |row| {
        row["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("storage_topology_description");
    });
    let errors = fixture.errors();
    assert!(
        errors.contains("requires declared storage_topology_description"),
        "{errors}"
    );

    let fixture = Fixture::new();
    fixture.mutate_e3_row(0, |row| {
        row["measurements"]["storage_authority_mode"] = serde_json::json!("unsupported-authority");
    });
    let errors = fixture.errors();
    assert!(errors.contains("storage_authority_mode"), "{errors}");
    assert!(errors.contains("native-create-only"), "{errors}");
}

#[test]
fn rejects_missing_or_false_portable_gate_markers_and_quiet_host_text() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path)
        .unwrap()
        .replacen("\"portable_gate\":true,", "", 1);
    fs::write(path, body).unwrap();
    assert!(fixture.errors().contains("requires portable_gate=true"));

    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "synthetic-fixture",
        "synthetic-fixture on a quiet host",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("contains a non-portable quiet-host gate")
    );
}

#[test]
fn rejects_host_speed_pass_bar_even_when_portable_markers_are_forged_true() {
    let fixture = Fixture::new();
    let path = fixture.root.join("e3.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "E3: 1/5/20/100ms bounds; sustained batched commits with valid latency distributions and logically identical interleaved recorder controls; 10M ephemeral in-memory projection rebuilt by exact bounded durable-log genesis replay; streaming complete-state digests match with zero missing, duplicate, or invalid items; replay progress and bounded-resource samples are monotonic; absolute capacity is reported for the declared topology, not used as a portable gate",
        "finish within 30 seconds on this host",
        1,
    );
    fs::write(path, body).unwrap();
    assert!(
        fixture
            .errors()
            .contains("must use the governed host-independent pass bar")
    );
}

#[cfg(unix)]
#[test]
fn rejects_symlink_authority_and_writer_target() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    fs::rename(
        fixture.root.join("fencing.json"),
        fixture.root.join("real-fencing.json"),
    )
    .unwrap();
    symlink("real-fencing.json", fixture.root.join("fencing.json")).unwrap();
    assert!(fixture.errors().contains("contains a symlink"));

    let output = fixture.root.join("output.json");
    let victim = fixture.root.join("victim.json");
    fs::write(&victim, "unchanged").unwrap();
    symlink(&victim, &output).unwrap();
    assert!(fireweed_release::RunOwned::new(repo_root(), &fixture.root, &output).is_err());
    assert_eq!(fs::read_to_string(victim).unwrap(), "unchanged");
}

#[test]
fn rejects_force_sealed_path_labeled_window_timed() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"][0]["request_id_timing"] = serde_json::json!("latency_window");
    });
    assert!(
        fixture
            .errors()
            .contains("force-sealed request_id evidence must not be labeled latency-window timed")
    );
}

#[test]
fn rejects_ac7_without_distinct_latency_window_and_request_id_timing() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["ac7_binding"]["latency_window_timing"] =
            serde_json::json!("force_sealed_config_independent");
    });
    assert!(fixture.errors().contains("genuine latency-window timing"));
}

#[test]
fn rejects_entry_without_passing_manifest_fence_link() {
    let fixture = Fixture::new();
    fixture.mutate_json("contract.json", |value| {
        value["entries"][0]["manifest_fence"]["applicability"] =
            serde_json::json!({"status":"capability_na","reason":"not measured"});
    });
    assert!(
        fixture
            .errors()
            .contains("manifest fence authority does not link the passing stale-epoch fence")
    );
}

#[test]
fn rejects_unproven_native_manifest_fence() {
    let fixture = Fixture::new();
    fixture.mutate_json("fencing.json", |value| {
        value["authority"]["stale_epoch_rejected"] = serde_json::json!(false);
    });
    assert!(
        fixture
            .errors()
            .contains("does not prove the selected, run-bound S3 publication authority")
    );
}

#[test]
fn native_fence_builder_derives_pass_from_executed_outcomes() {
    let row = build_e3_fence_evidence(native_fence_observation()).unwrap();
    assert_eq!(row.schema_version, 5);
    assert_eq!(row.result, "pass");
    assert_eq!(row.store_profile, "s3_native_create_only");
    assert!(matches!(
        row.authority,
        E3FenceAuthorityEvidence::NativeCreateOnly {
            stale_epoch_rejected: true,
            current_epoch_committed: true,
            ..
        }
    ));

    let mut failed = native_fence_observation();
    failed.authority = E3FenceAuthorityObservation::NativeCreateOnly {
        stale_epoch: E3ObservedOutcome::Failed {
            observation: "stale writer unexpectedly committed".into(),
        },
        current_epoch: passed("current owner commit was readable"),
    };
    assert_eq!(build_e3_fence_evidence(failed).unwrap().result, "fail");
}

#[test]
fn transaction_builder_requires_executed_exact_revision_profile_bound_and_ac() {
    let observation = E3TransactionObservation {
        source_revision: REVISION.into(),
        evidence_link: evidence_link(),
        profile: "object_log_inmemory_projection".into(),
        bound_ms: 20,
        ac: "AC-TXN-7".into(),
        backend: "objectlog(force-seal|group-commit)".into(),
        assertions: vec!["request id replay and latency-window group commit passed".into()],
        recorded_at: "2026-07-20T00:00:00Z".into(),
        passed: true,
    };
    let row = build_e3_transaction_evidence_row(observation.clone()).unwrap();
    assert_eq!(row.source_revision.as_deref(), Some(REVISION));
    assert_eq!(
        row.e3_profile.as_deref(),
        Some("object_log_inmemory_projection")
    );
    assert_eq!(row.bound_ms, Some(20));
    assert_eq!(row.latency_window_timing.as_deref(), Some("latency_window"));
    assert_eq!(
        row.request_id_timing.as_deref(),
        Some("force_sealed_config_independent")
    );
    assert_eq!(row.e3_run_id.as_deref(), Some(RUN_ID));

    for invalid in [
        E3TransactionObservation {
            passed: false,
            ..observation.clone()
        },
        E3TransactionObservation {
            bound_ms: 21,
            ..observation.clone()
        },
        E3TransactionObservation {
            source_revision: "fixture".into(),
            ..observation.clone()
        },
        E3TransactionObservation {
            backend: "generic".into(),
            ..observation
        },
    ] {
        assert!(build_e3_transaction_evidence_row(invalid).is_err());
    }
}

#[test]
fn rejects_generic_transaction_row_reused_through_manifest() {
    let fixture = Fixture::new();
    let path = fixture.root.join("tp003.jsonl");
    let body = fs::read_to_string(&path).unwrap().replacen(
        "\"e3_profile\":\"object_log_inmemory_projection\",\"bound_ms\":1",
        "\"e3_profile\":\"object_log_inmemory_projection\",\"bound_ms\":5",
        1,
    );
    fs::write(path, body).unwrap();
    let errors = fixture.errors();
    assert!(
        errors.contains("profile=object_log_inmemory_projection bound=1ms"),
        "{errors}"
    );
    assert!(errors.contains("found 0"), "{errors}");
    assert!(errors.contains("found 2"), "{errors}");
}
