use std::env;
use std::path::PathBuf;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_server::{Backend, Config, SegmentConfig, start};

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_duration_ms(key: &str, default_ms: u64) -> Duration {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(default_ms))
}

fn unsupported_storage(log: &str, projection: &str, reason: &str) -> ! {
    eprintln!(
        "unsupported storage configuration PQUEUE_LOG_BACKEND={log} PQUEUE_PROJECTION_BACKEND={projection}: {reason}"
    );
    std::process::exit(2);
}

fn parse_backend() -> Backend {
    let log = env_or("PQUEUE_LOG_BACKEND", "objectlog");
    let projection = env_or("PQUEUE_PROJECTION_BACKEND", "inmemory");

    match (log.as_str(), projection.as_str()) {
        ("memory", "inmemory") => Backend::Memory,
        ("sqlite", "inmemory") => Backend::Sqlite(PathBuf::from(env_or(
            "PQUEUE_SQLITE_LOG_PATH",
            "/var/lib/pqueue/pqueue-log.db",
        ))),
        ("objectlog", "inmemory") => {
            let object_root = PathBuf::from(env_or(
                "PQUEUE_OBJECT_LOG_ROOT",
                "/var/lib/pqueue/object-log",
            ));
            // `file` (default) = the per-command file `ObjectLogBackend`; `segmented` = the group-commit
            // substrate over an IN-MEMORY projection (Fix B): durable via the sealed log, fast apply.
            match env_or("PQUEUE_OBJECT_LOG_MODE", "file").as_str() {
                "file" => Backend::ObjectLog(object_root),
                "segmented" => {
                    let target_bytes = parse_usize("PQUEUE_SEGMENT_TARGET_BYTES", 262_144);
                    let max_latency_ms = parse_u64("PQUEUE_SEGMENT_MAX_LATENCY_MS", 20);
                    let config =
                        SegmentConfig::new(target_bytes, max_latency_ms).unwrap_or_else(|e| {
                            eprintln!("invalid segment configuration: {e}");
                            std::process::exit(2);
                        });
                    Backend::SegmentedObjectLogInMemory {
                        object_root,
                        config,
                    }
                }
                other => unsupported_storage(
                    &log,
                    &projection,
                    &format!("unknown PQUEUE_OBJECT_LOG_MODE={other:?}; expected file|segmented"),
                ),
            }
        }
        ("objectlog", "sqlite") => {
            let object_root = PathBuf::from(env_or(
                "PQUEUE_OBJECT_LOG_ROOT",
                "/var/lib/pqueue/object-log",
            ));
            let projection_path = PathBuf::from(env_or(
                "PQUEUE_SQLITE_PROJECTION_PATH",
                "/var/lib/pqueue/pqueue-projection.db",
            ));
            // `file` (default) preserves the per-command object-log path; `segmented` selects the
            // group-commit substrate (one sealed segment object + one batched SQLite apply per batch).
            match env_or("PQUEUE_OBJECT_LOG_MODE", "file").as_str() {
                "file" => Backend::ObjectLogSqlite {
                    object_root,
                    projection_path,
                },
                "segmented" => {
                    let target_bytes = parse_usize("PQUEUE_SEGMENT_TARGET_BYTES", 262_144);
                    let max_latency_ms = parse_u64("PQUEUE_SEGMENT_MAX_LATENCY_MS", 20);
                    let config =
                        SegmentConfig::new(target_bytes, max_latency_ms).unwrap_or_else(|e| {
                            eprintln!("invalid segment configuration: {e}");
                            std::process::exit(2);
                        });
                    Backend::SegmentedObjectLogSqlite {
                        object_root,
                        projection_path,
                        config,
                    }
                }
                other => unsupported_storage(
                    &log,
                    &projection,
                    &format!("unknown PQUEUE_OBJECT_LOG_MODE={other:?}; expected file|segmented"),
                ),
            }
        }
        #[cfg(feature = "postgres")]
        ("postgres", "inmemory") => Backend::PostgresNative {
            url: env_or(
                "PQUEUE_PG_URL",
                "postgres://postgres@127.0.0.1:5432/postgres",
            ),
        },
        #[cfg(not(feature = "postgres"))]
        ("postgres", "inmemory") => unsupported_storage(
            &log,
            &projection,
            "postgres adapter is wired through the blocking-safe PostgresNativeBackend, but this binary \
             was built without the `postgres` cargo feature; rebuild with `--features postgres` (or \
             `--features postgres,tls` for native-tls)",
        ),
        (_, "sqlite" | "postgres") => unsupported_storage(
            &log,
            &projection,
            "the requested projection backend is not wired by pqueue-server yet",
        ),
        _ => unsupported_storage(
            &log,
            &projection,
            "supported wired combinations are memory/inmemory, sqlite/inmemory, and objectlog/inmemory",
        ),
    }
}

fn queue_definition(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap_or_else(|e| {
            eprintln!("invalid tenant id in PQUEUE_BOOTSTRAP_QUEUES: {e}");
            std::process::exit(2);
        }),
        queue_id: QueueId::new(queue).unwrap_or_else(|e| {
            eprintln!("invalid queue id in PQUEUE_BOOTSTRAP_QUEUES: {e}");
            std::process::exit(2);
        }),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    }
}

fn parse_bootstrap_queues() -> Vec<QueueDefinition> {
    env_or("PQUEUE_BOOTSTRAP_QUEUES", "t1:q1")
        .split(',')
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }
            let (tenant, queue) = trimmed.split_once(':').unwrap_or_else(|| {
                eprintln!(
                    "invalid PQUEUE_BOOTSTRAP_QUEUES entry {trimmed:?}; expected tenant:queue"
                );
                std::process::exit(2);
            });
            Some(queue_definition(tenant, queue))
        })
        .collect()
}

// Multi-threaded runtime: blocking durable work (segment seal I/O + the batched SQLite apply) runs on a
// worker thread without stalling the network accept/read path on the others, so concurrent pushes from many
// RESP connections keep co-buffering into the next segment while one is sealing (the group-commit win).
//
// `PQUEUE_WORKER_THREADS` caps the tokio worker-thread pool (default: one per available core). A node owns
// only its own queues and is single-writer per queue, so a small pool suffices; capping it is important when
// many nodes are CO-LOCATED on one host (e.g. a dense multi-owner box), where the default per-process
// `num_cpus` pool would oversubscribe the shared cores and degrade every node's throughput.
fn main() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = env::var("PQUEUE_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
    {
        builder.worker_threads(n);
    }
    builder
        .build()
        .expect("build tokio runtime")
        .block_on(async_main());
}

async fn async_main() {
    if env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("pqueue-service {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "pqueue-service\n\nEnvironment:\n  PQUEUE_LISTEN_ADDR=0.0.0.0:8080\n  PQUEUE_LOG_BACKEND=objectlog|postgres|sqlite|memory\n  PQUEUE_PROJECTION_BACKEND=inmemory|sqlite|postgres\n  PQUEUE_NODE_ID=0           (per-replica id; distinct integer per instance, else hashed to a byte)\n  PQUEUE_SQLITE_LOG_PATH=/var/lib/pqueue/pqueue-log.db\n  PQUEUE_OBJECT_LOG_ROOT=/var/lib/pqueue/object-log\n  PQUEUE_PG_URL=postgres://user:pass@host:5432/db   (postgres backend; build --features postgres[,tls])\n  PQUEUE_SQLITE_PROJECTION_PATH=/var/lib/pqueue/pqueue-projection.db\n  PQUEUE_OBJECT_LOG_MODE=file|segmented   (objectlog+sqlite or objectlog+inmemory; file=per-command, segmented=group-commit)\n  PQUEUE_SEGMENT_TARGET_BYTES=262144      (segmented: byte-size seal trigger)\n  PQUEUE_SEGMENT_MAX_LATENCY_MS=20        (segmented: latency seal trigger)\n  PQUEUE_RECOVERY_MAX_TAIL_COMMANDS=1000000  (object_log_sqlite: recovery-window budget; reopen replays only the object-log tail beyond the projection snapshot high-water, warning if it exceeds this)\n  PQUEUE_BOOTSTRAP_QUEUES=t1:q1[,tenant:queue]\n  PQUEUE_RECLAIM_INTERVAL_MS=1000"
        );
        return;
    }

    let listen = env_or("PQUEUE_LISTEN_ADDR", "0.0.0.0:8080");
    let log_backend = env_or("PQUEUE_LOG_BACKEND", "objectlog");
    let projection_backend = env_or("PQUEUE_PROJECTION_BACKEND", "inmemory");
    let config = Config {
        backend: parse_backend(),
        node_id: pqueue_server::resolve_node_id(&env_or("PQUEUE_NODE_ID", "0")),
        listen,
        reclaim_interval: parse_duration_ms("PQUEUE_RECLAIM_INTERVAL_MS", 1_000),
        queues: parse_bootstrap_queues(),
    };

    match start(config).await {
        Ok(server) => {
            eprintln!(
                "pqueue-service {} listening on {} with log={} projection={}",
                env!("CARGO_PKG_VERSION"),
                server.addr(),
                log_backend,
                projection_backend
            );
            std::future::pending::<()>().await;
        }
        Err(e) => {
            eprintln!("pqueue-service failed to start: {e}");
            std::process::exit(1);
        }
    }
}
