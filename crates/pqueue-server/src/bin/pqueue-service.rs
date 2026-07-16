//! The `pqueue-service` binary: the composition root's executable entry point.
//!
//! This is the ONLY place that reads the live process environment. `main` does exactly one env thing —
//! collect `std::env::vars()` into a map — then hands it to the single optional populator
//! [`Config::from_env`], builds the tokio runtime from the resulting typed [`Config`], and runs the server
//! via [`start`]. All env-NAME knowledge lives in `Config::from_env` (the `env-config` feature of the
//! library); the library's `Config` + `start`/`start_with_ownership` carry no environment dependency.

use std::collections::BTreeMap;

use pqueue_server::{Config, start};

const HELP: &str = "pqueue-service\n\nEnvironment:\n  PQUEUE_LISTEN_ADDR=0.0.0.0:8080\n  PQUEUE_ADVERTISE_ADDR=10.0.0.12:8080      (required for replicas>1; pod-reachable IP:port)\n  PQUEUE_LOG_BACKEND=objectlog|postgres|sqlite|memory\n  PQUEUE_PROJECTION_BACKEND=inmemory|sqlite|hybrid|hybrid-async|postgres\n  PQUEUE_CONTROL_PLANE=inprocess|postgres   (inprocess is development-only and requires one replica)\n  PQUEUE_REPLICA_COUNT=1                    (>1 requires the postgres control plane)\n  PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL=postgres://user:pass@host:5432/db\n  PQUEUE_CONTROL_PLANE_HEARTBEAT_TTL_MS=5000\n  PQUEUE_CONTROL_PLANE_LEASE_TTL_MS=15000\n  PQUEUE_NODE_ID=0           (per-replica id; distinct integer per instance, else hashed to a byte)\n  PQUEUE_SQLITE_LOG_PATH=/var/lib/pqueue/pqueue-log.db\n  PQUEUE_OBJECT_LOG_ROOT=/var/lib/pqueue/object-log\n  PQUEUE_PG_URL=postgres://user:pass@host:5432/db   (postgres backend; build --features postgres[,tls])\n  PQUEUE_POSTGRES_LOG_DATABASE_URL=...   (Helm/Lakebase DSN secret; preferred over PQUEUE_PG_URL; sslmode=require needs --features tls)\n  DATABRICKS_HOST/...=...   (optional Databricks service-principal|PAT credential injection for the postgres backend)\n  PQUEUE_SQLITE_PROJECTION_PATH=/var/lib/pqueue/pqueue-projection.db   (objectlog log + sqlite, hybrid, or hybrid-async projection)\n  PQUEUE_FJORD_STATE_ROOT=/var/lib/pqueue/fjord   (embedded fjord storage namespace root, separate from queue storage)\n  PQUEUE_FJORD_CLUSTER_ID=pqueue-fjord            (embedded fjord cluster id)\n  PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS=100000   (objectlog/hybrid-async async-apply thresholds; each bound must be >0)\n  PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES=536870912\n  PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX=1024\n  PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS=60000\n  PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD=3\n  PQUEUE_WORKER_THREADS=N                  (cap the tokio worker-thread pool; default one per core)\n  PQUEUE_BOOTSTRAP_QUEUES=t1:q1[,tenant:queue]\n  PQUEUE_CHANGE_RECORD_SINK_ENABLED=1
  PQUEUE_CHANGE_RECORD_SINK_ENDPOINT=http://127.0.0.1:8081/ingest
  PQUEUE_CHANGE_RECORD_SINK_TICK_INTERVAL_MS=250
  PQUEUE_CHANGE_RECORD_SINK_BATCH_SIZE=256
  PQUEUE_CHANGE_RECORD_SINK_AUTHORIZATION=Bearer token
  PQUEUE_CHANGE_RECORD_SINK_HEADER_X_API_KEY=...
  PQUEUE_RECLAIM_INTERVAL_MS=1000";

const OBJECT_LOG_HELP: &str = "Object-log storage profiles:\n  PQUEUE_OBJECT_LOG_STORE=local|s3   (local is single-replica only; s3 is shared)\n  PQUEUE_OBJECT_LOG_ROOT=/var/lib/pqueue/object-log   (local only)\n  PQUEUE_OBJECTLOG_BUFFERED_BYTES_GLOBAL=67108864\n  PQUEUE_OBJECTLOG_BUFFERED_BYTES_TENANT=33554432   (optional uniform tenant cap)\n  PQUEUE_OBJECTLOG_QUEUE_WAITING_BYTES=16777216\n  PQUEUE_OBJECT_LOG_S3_ENDPOINT=https://s3.example.com\n  PQUEUE_OBJECT_LOG_S3_BUCKET=pqueue\n  PQUEUE_OBJECT_LOG_S3_REGION=us-east-1\n  PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE=static\n  PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID=...\n  PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY=...\n  PQUEUE_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP=false   (local MinIO only)";

// Multi-threaded runtime: blocking durable work (segment seal I/O + the batched SQLite apply) runs on a
// worker thread without stalling the network accept/read path on the others, so concurrent pushes from many
// RESP connections keep co-buffering into the next segment while one is sealing (the group-commit win).
//
// `PQUEUE_WORKER_THREADS` caps the tokio worker-thread pool (default: one per available core). A node owns
// only its own queues and is single-writer per queue, so a small pool suffices; capping it is important when
// many nodes are CO-LOCATED on one host (e.g. a dense multi-owner box), where the default per-process
// `num_cpus` pool would oversubscribe the shared cores and degrade every node's throughput.
fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("pqueue-service {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("{HELP}\n\n{OBJECT_LOG_HELP}");
        return;
    }

    // The ONE process-environment read in the whole codebase's runtime path: collect the live env into a
    // plain map, then let the single optional populator map the documented names onto the typed Config.
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let config = match Config::from_env(&env) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let runtime_resource_metrics_path = env.get("PQUEUE_RUNTIME_RESOURCE_METRICS_PATH").cloned();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = config.worker_threads {
        builder.worker_threads(n);
    }
    builder
        .build()
        .expect("build tokio runtime")
        .block_on(run(config, runtime_resource_metrics_path));
}

async fn run(config: Config, runtime_resource_metrics_path: Option<String>) {
    let listen = config.listen.clone();
    match start(config).await {
        Ok(server) => {
            if let Some(path) = runtime_resource_metrics_path {
                tokio::spawn(report_runtime_resources(path));
            }
            eprintln!(
                "pqueue-service {} listening on {} (configured listen={})",
                env!("CARGO_PKG_VERSION"),
                server.addr(),
                listen,
            );
            std::future::pending::<()>().await;
        }
        Err(e) => {
            eprintln!("pqueue-service failed to start: {e}");
            std::process::exit(1);
        }
    }
}

/// Export authoritative Tokio runtime gauges for the live process. `num_alive_tasks` includes detached
/// object-log flushers, the server background loops, and per-connection handler tasks, so the density
/// proof does not substitute OS file descriptors for async work. Rename makes each JSON snapshot atomic.
async fn report_runtime_resources(path: String) {
    let tmp = format!("{path}.tmp");
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        tick.tick().await;
        let metrics = tokio::runtime::Handle::current().metrics();
        let snapshot = serde_json::json!({
            "tokio_worker_threads": metrics.num_workers(),
            "tokio_alive_tasks": metrics.num_alive_tasks(),
        });
        if std::fs::write(&tmp, snapshot.to_string()).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }
}
