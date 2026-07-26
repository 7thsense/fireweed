use std::path::PathBuf;

use fireweed_release::transaction::{evidence_paths, verify_transaction_evidence};

fn fixture(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/ci/fixtures/transaction-evidence")
        .join(path)
}

#[test]
fn transaction_evidence_accepts_both_profiles_and_all_required_acs() {
    let summary = verify_transaction_evidence(&evidence_paths([
        fixture("valid/matrix.jsonl"),
        fixture("valid/parity.jsonl"),
    ]))
    .expect("valid exact-pair evidence passes");
    assert_eq!(summary.rows, 8);
    assert_eq!(summary.satisfied.len(), 8);
}

#[test]
fn transaction_evidence_rejects_profile_omission() {
    let errors = verify_transaction_evidence(&evidence_paths([fixture("profile-omission.jsonl")]))
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| { error.0.contains("profile=postgres/postgres ac=AC-TXN-1") })
    );
}

#[test]
fn transaction_evidence_rejects_ac_omission() {
    let errors =
        verify_transaction_evidence(&evidence_paths([fixture("ac-omission.jsonl")])).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| { error.0.contains("profile=postgres/postgres ac=AC-TXN-6") })
    );
}

#[test]
fn transaction_evidence_rejects_non_pass_result() {
    let errors =
        verify_transaction_evidence(&evidence_paths([fixture("failure.jsonl")])).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| { error.0.contains("result must be pass") })
    );
}

#[test]
fn transaction_evidence_rejects_unjustified_na() {
    let errors = verify_transaction_evidence(&evidence_paths([fixture("unjustified-na.jsonl")]))
        .unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.0.contains("row-level n/a is not authorized"))
    );
}

#[test]
fn transaction_evidence_rejects_bogus_structured_na() {
    for name in ["bogus-covered-by-na.jsonl", "duplicate-field-na.jsonl"] {
        let errors = verify_transaction_evidence(&evidence_paths([fixture(name)])).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.0.contains("row-level n/a is not authorized")),
            "{name} unexpectedly passed: {errors:?}"
        );
    }
}

#[test]
fn exact_pair_local_gate_requires_fresh_nonempty_evidence() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source =
        std::fs::read_to_string(repo.join("scripts/ci/record-postgres-transaction-evidence.sh"))
            .unwrap();
    let fresh = source
        .find("rm -f \"$matrix_evidence\" \"$parity_evidence\"")
        .unwrap();
    let matrix_test = source
        .find("ac_txn_contract_matrix_postgres_storage_pairs -- --exact --nocapture")
        .unwrap();
    let parity_test = source
        .find("ac_txn_6_postgres_storage_pair_parity -- --exact --nocapture")
        .unwrap();
    let matrix_nonempty = source.find("test -s \"$matrix_evidence\"").unwrap();
    let parity_nonempty = source.find("test -s \"$parity_evidence\"").unwrap();
    let verify = source
        .find("--bin fireweed-verify-transaction-evidence")
        .unwrap();
    assert!(fresh < matrix_test);
    assert!(fresh < parity_test);
    assert!(matrix_test < matrix_nonempty && parity_test < parity_nonempty);
    assert!(matrix_nonempty < verify && parity_nonempty < verify);
}
