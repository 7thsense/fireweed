//! Private-cluster crash recovery qualification. No caller-supplied database URL
//! or preexisting cluster is stopped. Immediate shutdown aborts server sessions
//! without a clean shutdown checkpoint; it is not a storage power-loss test.
#![cfg(unix)]
use fireweed_core::queue_contract::*;
use fireweed_engine::{Clock, queue_contract::QueueStore};
use fireweed_postgres::PostgresQueueStore;
use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}
struct PrivateCluster {
    root: PathBuf,
    data: PathBuf,
    log: PathBuf,
    bin: PathBuf,
    port: u16,
    running: bool,
}
impl PrivateCluster {
    fn new(bin: &Path) -> Self {
        let bin = bin.canonicalize().expect("PostgreSQL binary directory");
        for name in ["initdb", "pg_ctl", "postgres"] {
            assert!(
                bin.join(name).is_file(),
                "missing private-cluster binary {name}"
            );
        }
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&parent).unwrap();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = parent.join(format!(
            "api007-server-crash-{}-{nonce}",
            std::process::id()
        ));
        // create_dir rejects any existing path; only this owned directory may be cleaned.
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let port = TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let cluster = Self {
            data: root.join("data"),
            log: root.join("server.log"),
            root,
            bin: bin.to_owned(),
            port,
            running: false,
        };
        cluster
            .command("initdb")
            .args([
                "--no-locale",
                "--encoding=UTF8",
                "--auth=trust",
                "--username=erik",
            ])
            .arg("-D")
            .arg(&cluster.data)
            .output()
            .map(|out| {
                assert!(
                    out.status.success(),
                    "private initdb failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                )
            })
            .unwrap();
        cluster
    }
    fn command(&self, name: &str) -> Command {
        let mut cmd = Command::new(self.bin.join(name));
        cmd.env_remove("PGOPTIONS")
            .env("LC_ALL", "C")
            .current_dir(&self.root);
        cmd
    }
    fn start(&mut self) {
        let options = format!(
            "-h 127.0.0.1 -p {} -c unix_socket_directories='' -c fsync=on -c full_page_writes=on -c synchronous_commit=on",
            self.port
        );
        // Set before starting so a partially successful start is still owned by Drop.
        self.running = true;
        let out = self
            .command("pg_ctl")
            .arg("-D")
            .arg(&self.data)
            .arg("-l")
            .arg(&self.log)
            .args(["-w", "-t", "15", "-o", &options, "start"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "private PostgreSQL start failed: {}\n{}",
            String::from_utf8_lossy(&out.stderr),
            std::fs::read_to_string(&self.log).unwrap_or_default()
        );
    }
    fn url(&self) -> String {
        format!("postgresql://erik@127.0.0.1:{}/postgres", self.port)
    }
    async fn verify_durable_settings(&self) {
        let (client, connection) = tokio_postgres::connect(&self.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let data = client
            .query_one("SHOW data_directory", &[])
            .await
            .unwrap()
            .get::<_, String>(0);
        assert_eq!(Path::new(&data), self.data, "connected to wrong cluster");
        for setting in ["fsync", "full_page_writes", "synchronous_commit"] {
            let row = client
                .query_one(&format!("SHOW {setting}"), &[])
                .await
                .unwrap();
            assert_eq!(row.get::<_, String>(0), "on", "{setting}");
        }
        drop(client);
        driver.await.unwrap().unwrap();
    }
    fn crash(&mut self) {
        assert!(self.running);
        // Stock pg_ctl directs abrupt SIGQUIT only to this owned data directory.
        // Unlike smart/fast stop, immediate stop does not make a clean checkpoint.
        let out = self
            .command("pg_ctl")
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "immediate", "-w", "-t", "15", "stop"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "private immediate shutdown failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        self.running = false;
    }
}
impl Drop for PrivateCluster {
    fn drop(&mut self) {
        if self.running {
            let stopped = self
                .command("pg_ctl")
                .arg("-D")
                .arg(&self.data)
                .args(["-m", "immediate", "-w", "-t", "15", "stop"])
                .output();
            if !stopped.is_ok_and(|o| o.status.success()) {
                eprintln!(
                    "Private PostgreSQL cleanup could not confirm stop; retained {}",
                    self.root.display()
                );
                return;
            }
            self.running = false;
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[tokio::test]
#[ignore = "requires FIREWEED_API007_PG_BIN; creates and crashes a private PostgreSQL cluster"]
async fn acknowledged_atomic_group_survives_postgresql_crash_recovery() {
    let bin = std::env::var_os("FIREWEED_API007_PG_BIN")
        .expect("explicit PostgreSQL binary directory for isolated crash fixture");
    let mut cluster = PrivateCluster::new(Path::new(&bin));
    cluster.start();
    cluster.verify_durable_settings().await;
    let store = PostgresQueueStore::connect(
        &cluster.url(),
        Arc::new(FixedClock),
        QueueLimits::default(),
        2,
    )
    .await
    .unwrap();
    let batch = fireweed_conformance::queue_contract::prepare_process_loss(
        &store,
        QueueAddress {
            tenant: "api007-private-server-crash".into(),
            queue: "acknowledged".into(),
        },
    )
    .await;
    // Do not shut down the store or request a checkpoint before crashing PostgreSQL.
    cluster.crash();
    drop(store);
    cluster.start();
    cluster.verify_durable_settings().await;
    let log = std::fs::read_to_string(&cluster.log).unwrap();
    assert!(
        log.contains("database system was interrupted"),
        "no interrupted-system evidence: {log}"
    );
    assert!(
        log.contains("automatic recovery in progress"),
        "no WAL crash recovery evidence: {log}"
    );
    let reopened = PostgresQueueStore::connect(
        &cluster.url(),
        Arc::new(FixedClock),
        QueueLimits::default(),
        2,
    )
    .await
    .unwrap();
    fireweed_conformance::queue_contract::verify_process_loss(&reopened, batch).await;
    reopened.shutdown().await.unwrap();
    cluster.crash();
    std::fs::remove_dir_all(&cluster.root).expect("remove stopped owned cluster");
}
