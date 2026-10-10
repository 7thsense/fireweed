//! Fixture binding only. S3/Turso must run the same queue_benchmark::run workload.
use fireweed_core::queue_contract::*;
use fireweed_engine::{Clock, queue_contract::QueueStore};
use fireweed_postgres::PostgresQueueStore;
use std::sync::Arc;
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
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
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
    let mut warmup = config.clone();
    warmup.records = 64;
    fireweed_conformance::queue_benchmark::run(
        &store,
        QueueAddress {
            tenant: "api007-benchmark".into(),
            queue: format!("warmup-{nonce}"),
        },
        warmup,
    )
    .await;
    let report = fireweed_conformance::queue_benchmark::run(
        &store,
        QueueAddress {
            tenant: "api007-benchmark".into(),
            queue: nonce.to_string(),
        },
        config,
    )
    .await;
    println!("{}",serde_json::to_string(&serde_json::json!({"adapter":"native-postgresql","source_commit":source,"source_dirty":dirty,"rustc":rustc,"host":host,"build_profile":if cfg!(debug_assertions) {"debug"} else {"release"},"clock":"fixed0 (lease logic deterministic; wall time measures operations)","concurrency":1,"pool_size":2,"warmup_records":64,"workload":report})).unwrap());
    store.shutdown().await.unwrap();
}
