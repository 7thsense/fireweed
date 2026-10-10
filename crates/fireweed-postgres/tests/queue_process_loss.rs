//! Adapter fixture only; all acknowledged-effect assertions live in the shared suite.
use fireweed_core::queue_contract::*;
use fireweed_engine::{Clock, queue_contract::QueueStore};
use fireweed_postgres::PostgresQueueStore;
use std::{
    process::{Child, Command},
    sync::Arc,
    time::{Duration, Instant},
};
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}
struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
#[ignore = "internal child entry point for isolated PostgreSQL process-loss fixture"]
async fn process_loss_child() {
    let mode = std::env::var("FIREWEED_API007_CRASH_MODE").expect("run parent fixture");
    let payload = std::env::var("FIREWEED_API007_CRASH_PAYLOAD").unwrap();
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
    match mode.as_str() {
        "write" => {
            let queue = QueueAddress {
                tenant: "api007-process-loss".into(),
                queue: std::env::var("FIREWEED_API007_CRASH_QUEUE").unwrap(),
            };
            let batch =
                fireweed_conformance::queue_contract::prepare_process_loss(&store, queue).await;
            // This is a test-control acknowledgement, not part of database durability.
            let staging = format!("{payload}.staging");
            std::fs::write(&staging, postcard::to_allocvec(&batch).unwrap()).unwrap();
            std::fs::rename(staging, &payload).unwrap();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        "verify" => {
            let batch = postcard::from_bytes(&std::fs::read(payload).unwrap()).unwrap();
            fireweed_conformance::queue_contract::verify_process_loss(&store, batch).await;
            store.shutdown().await.unwrap();
        }
        _ => panic!("invalid fixture mode"),
    }
}
#[test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL; kills writer without shutdown"]
fn acknowledged_atomic_group_survives_writer_process_loss() {
    std::env::var("FIREWEED_API007_PG_TEST_URL").expect("isolated PostgreSQL fixture URL");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let path = std::env::temp_dir().join(format!(
        "fireweed-api007-crash-{}-{nonce}",
        std::process::id()
    ));
    let child = |mode: &str| {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "process_loss_child", "--ignored", "--nocapture"])
            .env("FIREWEED_API007_CRASH_MODE", mode)
            .env("FIREWEED_API007_CRASH_PAYLOAD", &path)
            .env("FIREWEED_API007_CRASH_QUEUE", &nonce);
        command
    };
    let mut writer = KillOnDrop(child("write").spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            writer.0.try_wait().unwrap().is_none(),
            "writer exited before acknowledgement"
        );
        assert!(
            Instant::now() < deadline,
            "writer acknowledgement timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    writer.0.kill().unwrap();
    let status = writer.0.wait().unwrap();
    assert!(
        !status.success(),
        "writer must terminate without clean shutdown"
    );
    let mut reader = KillOnDrop(child("verify").spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = reader.0.try_wait().unwrap() {
            assert!(status.success(), "fresh process verification failed");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fresh process verification timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::remove_file(path).unwrap();
}
