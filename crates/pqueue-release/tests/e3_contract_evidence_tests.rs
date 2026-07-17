use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_release::e3_contract::{
    E3FenceObservation, build_e3_fence_evidence, verify_e3_contract,
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

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
        verify_e3_contract(&self.manifest())
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
    let summary = verify_e3_contract(&fixture.manifest()).unwrap();
    assert_eq!(summary.entries, 8);
    assert_eq!(summary.transaction_rows, 9);
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
