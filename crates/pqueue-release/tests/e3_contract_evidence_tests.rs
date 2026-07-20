use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_release::e3_contract::{
    E3FenceObservation, build_e3_contract_manifest, build_e3_fence_evidence, verify_e3_contract,
    write_e3_contract, write_e3_fence_evidence,
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "pqueue-e3-contract-{}-{}",
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

    fn mutate_json(&self, name: &str, mutate: impl FnOnce(&mut serde_json::Value)) {
        let path = self.root.join(name);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        mutate(&mut value);
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
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
    assert_eq!(summary.transaction_rows, 9);
    assert_eq!(summary.cost_rows, 8);
}

#[test]
fn production_generator_builds_and_semantically_verifies_the_full_matrix() {
    let fixture = Fixture::new();
    let generated = fixture.root.join("generated.json");
    let manifest = build_e3_contract_manifest(
        REVISION.into(),
        "e3.jsonl".into(),
        "tp003.jsonl".into(),
        "fencing.json".into(),
    )
    .unwrap();
    write_e3_contract(&generated, &manifest).unwrap();
    let summary = verify_e3_contract(&generated, REVISION).unwrap();
    assert_eq!(
        (summary.entries, summary.transaction_rows, summary.cost_rows),
        (8, 9, 8)
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
    assert!(fixture.errors().contains("expected 2"));
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
    let row = build_e3_fence_evidence(E3FenceObservation {
        source_revision: REVISION.into(),
        stale_epoch_rejected: true,
        current_epoch_committed: true,
    })
    .unwrap();
    assert!(write_e3_fence_evidence(&output, &row).is_err());
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
fn rejects_unproven_manifest_fence_or_fallback() {
    let fixture = Fixture::new();
    fixture.mutate_json("fencing.json", |value| {
        value["stale_epoch_rejected"] = serde_json::json!(false);
    });
    assert!(
        fixture
            .errors()
            .contains("does not prove stale rejection/current commit")
    );

    let fixture = Fixture::new();
    fixture.mutate_json("fencing.json", |value| {
        value["no_cas"] = serde_json::json!({"status":"proven","reason":"fallback worked"});
    });
    assert!(fixture.errors().contains("authorized no-CAS exclusion"));
}

#[test]
fn fence_builder_fails_closed_and_emits_typed_release_profile() {
    let row = build_e3_fence_evidence(E3FenceObservation {
        source_revision: "0123456789abcdef0123456789abcdef01234567".into(),
        stale_epoch_rejected: true,
        current_epoch_committed: true,
    })
    .unwrap();
    assert_eq!(row.result, "pass");
    assert_eq!(row.store_profile, "minio_create_only_cas");

    assert!(
        build_e3_fence_evidence(E3FenceObservation {
            source_revision: "not-a-revision".into(),
            stale_epoch_rejected: true,
            current_epoch_committed: true,
        })
        .is_err()
    );
}
