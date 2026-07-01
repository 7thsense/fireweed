//! The `pqueue-service` binary: the composition root's executable entry point.
//!
//! This is the ONLY place that reads the live process environment. `main` does exactly one env thing —
//! collect `std::env::vars()` into a map — then hands it to the single optional populator
//! [`Config::from_env`], builds the tokio runtime from the resulting typed [`Config`], and runs the server
//! via [`start`]. All env-NAME knowledge lives in `Config::from_env` (the `env-config` feature of the
//! library); the library's `Config` + `start`/`start_with_ownership` carry no environment dependency.

use std::collections::BTreeMap;

use pqueue_server::{Config, start};

const HELP: &str = "pqueue-service\n\nEnvironment:\n  PQUEUE_LISTEN_ADDR=0.0.0.0:8080\n  PQUEUE_LOG_BACKEND=objectlog|postgres|sqlite|memory\n  PQUEUE_PROJECTION_BACKEND=inmemory|sqlite|hybrid|postgres\n  PQUEUE_NODE_ID=0           (per-replica id; distinct integer per instance, else hashed to a byte)\n  PQUEUE_SQLITE_LOG_PATH=/var/lib/pqueue/pqueue-log.db\n  PQUEUE_OBJECT_LOG_ROOT=/var/lib/pqueue/object-log\n  PQUEUE_PG_URL=postgres://user:pass@host:5432/db   (postgres backend; build --features postgres[,tls])\n  PQUEUE_POSTGRES_LOG_DATABASE_URL=...   (Helm/Lakebase DSN secret; preferred over PQUEUE_PG_URL; sslmode=require needs --features tls)\n  DATABRICKS_HOST/...=...   (optional Databricks service-principal|PAT credential injection for the postgres backend)\n  PQUEUE_SQLITE_PROJECTION_PATH=/var/lib/pqueue/pqueue-projection.db   (objectlog log + sqlite or hybrid projection)\n  PQUEUE_WORKER_THREADS=N                  (cap the tokio worker-thread pool; default one per core)\n  PQUEUE_BOOTSTRAP_QUEUES=t1:q1[,tenant:queue]\n  PQUEUE_RECLAIM_INTERVAL_MS=1000";

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
        println!("{HELP}");
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

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = config.worker_threads {
        builder.worker_threads(n);
    }
    builder
        .build()
        .expect("build tokio runtime")
        .block_on(run(config));
}

async fn run(config: Config) {
    let listen = config.listen.clone();
    match start(config).await {
        Ok(server) => {
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
