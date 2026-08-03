//! End-to-end: an evidence suite appends rows, the verifier validates them and asserts required evidence.

use std::collections::BTreeMap;

use fireweed_release::{
    LedgerRow, Measurements, ReleaseAuthority, ReleaseManifest, RunOwned, append_row,
    missing_evidence, verify_ledger, verify_release_manifest,
};

fn owned_ledger(tag: &str) -> RunOwned {
    let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let run_root = std::env::temp_dir().join(format!(
        "fireweed-release-owned-ledger-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&run_root);
    std::fs::create_dir_all(&run_root).unwrap();
    RunOwned::new(repository_root, &run_root, format!("{tag}.jsonl")).unwrap()
}

fn tmp_ledger(tag: &str) -> RunOwned {
    owned_ledger(&format!("raw-{tag}"))
}

fn row(suite: &str, exit: i32, evidence: &[&str]) -> LedgerRow {
    LedgerRow {
        suite: suite.into(),
        command: format!("cargo test {suite}"),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "release".into(),
        seed: 42,
        environment: "ci".into(),
        exit_status: exit,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "floor held".into(),
        evidence_tier: "release".into(),
        measurements: Measurements {
            tp002_evidence_ids: evidence.iter().map(|s| s.to_string()).collect(),
            values: BTreeMap::from([("items_per_sec".into(), serde_json::json!(154598))]),
        },
    }
}

#[test]
fn appended_rows_validate_and_evidence_is_asserted() {
    let path = owned_ledger("ok");

    append_row(&path, &row("object_log_commit_recovery_tests", 0, &["E3"])).unwrap();
    append_row(
        &path,
        &row("performance_cross_queue_scale_out_tests", 0, &["E2"]),
    )
    .unwrap();

    let summary = verify_ledger(path.path(), true).expect("strict validation passes");
    assert_eq!(summary.rows, 2);
    assert!(summary.evidence_ids.contains("E2") && summary.evidence_ids.contains("E3"));

    // E2+E3 present, but E0/E1 are not — the gate's require-evidence must report the gap.
    let missing = missing_evidence(&summary, &["E0", "E1", "E2", "E3"].map(String::from));
    assert_eq!(missing, vec!["E0".to_string(), "E1".to_string()]);

    let _ = std::fs::remove_dir_all(path.run_root());
}

#[test]
fn strict_validation_fails_a_failed_run_row() {
    let path = owned_ledger("failed");
    append_row(
        &path,
        &row("performance_cross_queue_scale_out_tests", 1, &["E2"]),
    )
    .unwrap();
    let errors = verify_ledger(path.path(), true).expect_err("a failed-run row must be rejected");
    assert!(errors.iter().any(|e| e.0.contains("exit_status")));
    let _ = std::fs::remove_dir_all(path.run_root());
}

#[test]
fn strict_rejects_missing_and_unknown_evidence_tiers() {
    let path = tmp_ledger("strict-tier");
    let mut unknown = row("tier", 0, &["E0"]);
    unknown.evidence_tier = "gold".into();
    path.write(format!("{}\n", unknown.to_jsonl())).unwrap();
    assert!(verify_ledger(path.path(), true).is_err());

    let mut raw = serde_json::to_value(row("tier", 0, &["E0"])).unwrap();
    raw.as_object_mut().unwrap().remove("evidence_tier");
    path.write(format!("{}\n", raw)).unwrap();
    assert!(verify_ledger(path.path(), true).is_err());
    let compatibility = verify_ledger(path.path(), false).unwrap();
    assert!(compatibility.evidence_ids.is_empty());
    assert!(compatibility.smoke_evidence_ids.is_empty());
    path.delete().unwrap();
}

#[test]
fn malformed_line_is_rejected() {
    let path = tmp_ledger("malformed");
    path.write(b"{not valid json}\n").unwrap();
    let errors = verify_ledger(path.path(), true).expect_err("malformed row rejected");
    assert!(errors.iter().any(|e| e.0.contains("malformed")));
    path.delete().unwrap();
}

#[test]
fn empty_ledger_fails_strict() {
    let path = tmp_ledger("empty");
    path.write(b"\n  \n").unwrap(); // only blank lines
    let errors = verify_ledger(path.path(), true).expect_err("an empty ledger is not evidence");
    assert!(errors.iter().any(|e| e.0.contains("empty")));
    path.delete().unwrap();
}

#[test]
fn missing_file_is_an_error() {
    let path = tmp_ledger("does-not-exist");
    path.delete().unwrap();
    let errors = verify_ledger(path.path(), true).expect_err("a missing ledger is an error");
    assert!(errors.iter().any(|e| e.0.contains("cannot open")));
}

fn release_manifest_dir(tag: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "fireweed-release-manifest-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn release_row(id: &str) -> LedgerRow {
    let mut row = row(&format!("tp002_{id}_release"), 0, &[id]);
    row.backend_profile = match id {
        "E0" | "E1" => "postgres_native",
        "E2" => fireweed_release::e2::RELEASE_BACKEND_PROFILE,
        "E3" => "object_log_sqlite_projection",
        _ => unreachable!(),
    }
    .into();
    row.measurements
        .values
        .insert("bars_met".into(), serde_json::json!(true));
    if matches!(id, "E0" | "E1") {
        row.measurements.values.extend([
            ("portable_gate".into(), serde_json::json!(true)),
            ("wall_clock_capacity_only".into(), serde_json::json!(true)),
            ("quiet_host_required".into(), serde_json::json!(false)),
            ("host_speed_gate".into(), serde_json::json!(false)),
        ]);
        row.pass_bar = "portable correctness, progress, and bounded resources".into();
    }
    row
}

fn valid_release_manifest() -> ReleaseManifest {
    ReleaseManifest {
        schema_version: 1,
        authorities: ["E0", "E1", "E2", "E3"]
            .into_iter()
            .map(|id| ReleaseAuthority {
                evidence_id: id.into(),
                path: format!("{id}.jsonl"),
            })
            .collect(),
    }
}

fn write_release_case(
    dir: &std::path::Path,
    manifest: &ReleaseManifest,
    rows: impl IntoIterator<Item = (String, LedgerRow)>,
) -> std::path::PathBuf {
    for (path, row) in rows {
        std::fs::write(dir.join(path), format!("{}\n", row.to_jsonl())).unwrap();
    }
    let manifest_path = dir.join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(manifest).unwrap()).unwrap();
    manifest_path
}

fn all_release_rows() -> Vec<(String, LedgerRow)> {
    ["E0", "E1", "E2", "E3"]
        .into_iter()
        .map(|id| (format!("{id}.jsonl"), release_row(id)))
        .collect()
}

#[test]
fn release_manifest_accepts_exact_semantic_e0_e3_authorities() {
    let dir = release_manifest_dir("valid");
    let path = write_release_case(&dir, &valid_release_manifest(), all_release_rows());
    let summary = verify_release_manifest(&path).expect("governed E0-E3 fixture passes");
    assert_eq!(summary.rows, 4);
    assert_eq!(
        summary.evidence_ids.into_iter().collect::<Vec<_>>(),
        ["E0", "E1", "E2", "E3"]
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn release_manifest_rejects_missing_file() {
    let dir = release_manifest_dir("missing");
    let mut missing_file = valid_release_manifest();
    missing_file.authorities[0].path = "absent.jsonl".into();
    let path = write_release_case(&dir, &missing_file, all_release_rows());
    let errors = verify_release_manifest(&path).unwrap_err();
    assert!(errors.iter().any(|error| error.0.contains("cannot open")));

    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn release_manifest_rejects_each_missing_e0_e3_authority_even_with_unlisted_substitute() {
    for missing in ["E0", "E1", "E2", "E3"] {
        let dir = release_manifest_dir(&format!("missing-{missing}"));
        let mut manifest = valid_release_manifest();
        manifest
            .authorities
            .retain(|authority| authority.evidence_id != missing);

        // The omitted row still exists beside the manifest. Exact manifest authority, not directory
        // presence, controls the governed set, so this unlisted substitute must never satisfy the ID.
        let path = write_release_case(&dir, &manifest, all_release_rows());
        let errors = verify_release_manifest(&path).unwrap_err();
        assert!(
            errors.iter().any(|error| error
                .0
                .contains(&format!("missing authority for {missing}"))),
            "missing {missing}: {errors:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn release_manifest_ignores_coexisting_tp003_jsonl_and_unlisted_tp002_substitution() {
    let dir = release_manifest_dir("mixed-contracts");
    let path = write_release_case(&dir, &valid_release_manifest(), all_release_rows());
    std::fs::write(
        dir.join("tp003-transaction-evidence.jsonl"),
        r#"{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"postgres/sqlite","result":"pass","detail":"fixture","assertions":["proof"],"recorded_at":"fixture"}
"#,
    )
    .unwrap();
    verify_release_manifest(&path)
        .expect("coexisting unlisted TP-003 is outside the TP-002 manifest");

    let mut manifest = valid_release_manifest();
    manifest
        .authorities
        .retain(|authority| authority.evidence_id != "E2");
    std::fs::write(&path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    // Both an unlisted E2 LedgerRow and an unrelated TP-003 JSONL remain in the directory.
    let errors = verify_release_manifest(&path).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("missing authority for E2"))
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn release_manifest_rejects_duplicate_id_and_duplicate_file_authority() {
    let dir = release_manifest_dir("duplicate");
    let mut manifest = valid_release_manifest();
    manifest.authorities.push(ReleaseAuthority {
        evidence_id: "E0".into(),
        path: "E0-copy.jsonl".into(),
    });
    manifest.authorities[3].path = "E2.jsonl".into();
    let mut rows = all_release_rows();
    rows.push(("E0-copy.jsonl".into(), release_row("E0")));
    let path = write_release_case(&dir, &manifest, rows);
    let errors = verify_release_manifest(&path).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("duplicate authority for evidence id E0"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("listed more than once"))
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn release_manifest_rejects_smoke_unknown_tier_and_non_release_scale() {
    for (tag, mutate, expected) in [
        (
            "smoke-tier",
            ("evidence_tier", "smoke"),
            "evidence_tier must be explicitly and exactly \"release\"",
        ),
        (
            "unknown-tier",
            ("evidence_tier", "gold"),
            "evidence_tier must be explicitly release or smoke",
        ),
        (
            "wrong-scale",
            ("scale", "in-process-smoke"),
            "scale must be exactly \"release\"",
        ),
    ] {
        let dir = release_manifest_dir(tag);
        let mut rows = all_release_rows();
        let row = &mut rows[2].1;
        match mutate.0 {
            "evidence_tier" => row.evidence_tier = mutate.1.into(),
            "scale" => row.scale = mutate.1.into(),
            _ => unreachable!(),
        }
        let path = write_release_case(&dir, &valid_release_manifest(), rows);
        let errors = verify_release_manifest(&path).unwrap_err();
        assert!(
            errors.iter().any(|error| error.0.contains(expected)),
            "{tag}: {errors:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    let dir = release_manifest_dir("missing-tier");
    let path = write_release_case(&dir, &valid_release_manifest(), all_release_rows());
    let mut raw = serde_json::to_value(release_row("E2")).unwrap();
    raw.as_object_mut().unwrap().remove("evidence_tier");
    std::fs::write(dir.join("E2.jsonl"), format!("{raw}\n")).unwrap();
    let errors = verify_release_manifest(&path).unwrap_err();
    assert!(errors.iter().any(|error| {
        error
            .0
            .contains("evidence_tier must be explicitly release or smoke")
    }));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn release_manifest_rejects_false_or_missing_bars_met() {
    for (tag, value, expected) in [
        ("bars-false", Some(serde_json::json!(false)), "boolean true"),
        ("bars-missing", None, "bars_met is required"),
    ] {
        let dir = release_manifest_dir(tag);
        let mut rows = all_release_rows();
        match value {
            Some(value) => {
                rows[1]
                    .1
                    .measurements
                    .values
                    .insert("bars_met".into(), value);
            }
            None => {
                rows[1].1.measurements.values.remove("bars_met");
            }
        }
        let path = write_release_case(&dir, &valid_release_manifest(), rows);
        let errors = verify_release_manifest(&path).unwrap_err();
        assert!(
            errors.iter().any(|error| error.0.contains(expected)),
            "{tag}: {errors:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn release_manifest_rejects_wrong_profile_and_unlisted_id_substitution() {
    let dir = release_manifest_dir("semantics");
    let mut rows = all_release_rows();
    rows[2].1.backend_profile = "object_log_inmemory_projection".into();
    rows[3].1.measurements.tp002_evidence_ids = vec!["E2".into()];
    let path = write_release_case(&dir, &valid_release_manifest(), rows);
    let errors = verify_release_manifest(&path).unwrap_err();
    assert!(errors.iter().any(|error| {
        error
            .0
            .contains("backend_profile \"object_log_inmemory_projection\" is not governed for E2; required E2 profile set is [\"object_log_sqlite_projection\"]")
    }));
    assert!(errors.iter().any(|error| {
        error
            .0
            .contains("required E2 profile set is [\"object_log_sqlite_projection\"]")
    }));
    assert!(errors.iter().any(|error| {
        error
            .0
            .contains("row evidence ids [\"E2\"] do not exactly match listed authority E3")
    }));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn release_manifest_rejects_parent_path_traversal() {
    let dir = release_manifest_dir("traversal");
    let mut manifest = valid_release_manifest();
    manifest.authorities[0].path = "../E0.jsonl".into();
    let path = write_release_case(&dir, &manifest, all_release_rows());
    let errors = verify_release_manifest(&path).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("not a safe manifest-relative path"))
    );
    std::fs::remove_dir_all(dir).unwrap();
}
