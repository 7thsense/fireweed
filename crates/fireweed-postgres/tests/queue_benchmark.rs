//! Fixture binding only. S3/Turso must run the same queue_benchmark::run workload.
use fireweed_core::queue_contract::*;
use fireweed_engine::Clock;
use fireweed_postgres::PostgresQueueStore;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Instant};
fn sha256(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn read_git(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}
#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL; controlled benchmark lane"]
async fn shared_settled_work_reference() {
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let source = git(&["rev-parse", "HEAD"]);
    let dirty = !git(&["status", "--porcelain"]).is_empty();
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .unwrap();
    assert!(rustc.status.success());
    let rustc = String::from_utf8(rustc.stdout).unwrap().trim().to_owned();
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .unwrap()
        .trim()
        .to_owned();
    let url =
        std::env::var("FIREWEED_API007_PG_TEST_URL").expect("isolated PostgreSQL fixture URL");
    let records = std::env::var("FIREWEED_API007_BENCH_RECORDS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(256);
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut hashes = serde_json::Map::new();
    for path in [
        "Cargo.lock",
        "crates/fireweed-bench/Cargo.lock",
        "crates/fireweed-conformance/src/queue_benchmark.rs",
        "crates/fireweed-postgres/tests/queue_benchmark.rs",
    ] {
        hashes.insert(
            path.into(),
            serde_json::json!(sha256(std::fs::read(root.join(path)).unwrap())),
        );
    }
    let diff = std::process::Command::new("git")
        .args(["diff", "HEAD", "--binary"])
        .output()
        .unwrap();
    assert!(diff.status.success());
    let dirty_patch_sha256 = sha256(diff.stdout);
    let construction = Instant::now();
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
    let construction_ns = construction.elapsed().as_nanos();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let config = fireweed_conformance::queue_benchmark::Workload {
        records,
        batch: 32,
        body_bytes: 256,
        state_bytes: 512,
        seed: 0x5eed,
        available_at: 0,
    };
    let outcome = fireweed_conformance::queue_benchmark::run_owned(
        &store,
        QueueAddress {
            tenant: "api007-benchmark".into(),
            queue: nonce.to_string(),
        },
        config,
        Some(QueueAddress {
            tenant: "api007-benchmark".into(),
            queue: format!("warmup-{nonce}"),
        }),
    )
    .await;
    let report = outcome.report;
    // One complete snapshot is captured before construction and checked after drain.
    let mut provenance_valid = true;
    for (path, expected) in &hashes {
        provenance_valid &=
            std::fs::read(root.join(path)).ok().map(sha256).as_deref() == expected.as_str();
    }
    let final_diff = std::process::Command::new("git")
        .args(["diff", "HEAD", "--binary"])
        .output()
        .ok();
    provenance_valid &= final_diff
        .filter(|output| output.status.success())
        .map(|output| sha256(output.stdout))
        .as_deref()
        == Some(dirty_patch_sha256.as_str());
    provenance_valid &= read_git(&["rev-parse", "HEAD"]).as_deref() == Some(source.as_str());
    provenance_valid &=
        read_git(&["status", "--porcelain"]).map(|status| !status.is_empty()) == Some(dirty);
    let mut report = report;
    if !provenance_valid {
        report["correctness"] = serde_json::json!("failed_source_changed");
        report["settled_per_second"] = serde_json::Value::Null;
        report["rate_scope"] =
            serde_json::json!("failed provenance check; no completed settlement rate");
    }
    println!("{}",serde_json::to_string(&serde_json::json!({"adapter":"native-postgresql","qualification":"internal-port reference smoke; not TP-005 acceptance","source_commit":source,"source_dirty":dirty,"source_hashes":hashes,"dirty_patch_sha256":dirty_patch_sha256,"rustc":rustc,"host":host,"build_profile":if cfg!(debug_assertions) {"debug"} else {"release"},"command":std::env::args().collect::<Vec<_>>(),"clock":"fixed0 (lease logic deterministic; wall time measures operations)","concurrency":1,"pool_size":2,"queue_limits":QueueLimits::default(),"construction_ns":construction_ns,"warmup_records":64,"workload":report})).unwrap());
    assert!(
        outcome.passed && provenance_valid,
        "reference workload or owned shutdown failed; evidence emitted above"
    );
}

#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL; benchmark failure/drain control"]
async fn failed_reference_retains_evidence_and_drains_postgres() {
    use fireweed_engine::queue_contract::QueueStore;
    let url =
        std::env::var("FIREWEED_API007_PG_TEST_URL").expect("isolated PostgreSQL fixture URL");
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
    let address = QueueAddress {
        tenant: "api007-benchmark-failure".into(),
        queue: "preflight".into(),
    };
    let outcome = fireweed_conformance::queue_benchmark::run_owned(
        &store,
        address.clone(),
        fireweed_conformance::queue_benchmark::Workload {
            records: 0,
            batch: 32,
            body_bytes: 256,
            state_bytes: 512,
            seed: 0x5eed,
            available_at: 0,
        },
        None,
    )
    .await;
    assert!(!outcome.passed);
    assert_eq!(outcome.report["correctness"], "failed");
    assert_eq!(outcome.report["shutdown"], "completed");
    assert!(
        outcome.report["workload_failure"]
            .as_str()
            .unwrap()
            .contains("config.records")
    );
    assert!(outcome.report["settled_per_second"].is_null());
    assert_eq!(
        store
            .stats(StatsRequest {
                queue: address,
                filter: Filter::default(),
                group_by: vec![],
                eligible_at: 0,
                buckets: None,
            })
            .await,
        Err(QueueError::Busy),
        "owned store must actually be closed after workload panic"
    );
    println!("{}", outcome.report);
}
