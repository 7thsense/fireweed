use std::env;
use std::path::PathBuf;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_server::{Backend, Config, start};

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
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
        ("objectlog", "inmemory") => Backend::ObjectLog(PathBuf::from(env_or(
            "PQUEUE_OBJECT_LOG_ROOT",
            "/var/lib/pqueue/object-log",
        ))),
        ("postgres", "inmemory") => unsupported_storage(
            &log,
            &projection,
            "postgres log exists as an adapter but is not wired into the tokio RESP server yet",
        ),
        ("objectlog", "sqlite") => unsupported_storage(
            &log,
            &projection,
            "objectlog plus sqlite projection is the intended storage shape, but this server still wires the file-backed object log to its in-memory projection",
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
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("pqueue-service {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "pqueue-service\n\nEnvironment:\n  PQUEUE_LISTEN_ADDR=0.0.0.0:8080\n  PQUEUE_LOG_BACKEND=objectlog|postgres|sqlite|memory\n  PQUEUE_PROJECTION_BACKEND=inmemory|sqlite|postgres\n  PQUEUE_SQLITE_LOG_PATH=/var/lib/pqueue/pqueue-log.db\n  PQUEUE_OBJECT_LOG_ROOT=/var/lib/pqueue/object-log\n  PQUEUE_BOOTSTRAP_QUEUES=t1:q1[,tenant:queue]\n  PQUEUE_RECLAIM_INTERVAL_MS=1000"
        );
        return;
    }

    let listen = env_or("PQUEUE_LISTEN_ADDR", "0.0.0.0:8080");
    let log_backend = env_or("PQUEUE_LOG_BACKEND", "objectlog");
    let projection_backend = env_or("PQUEUE_PROJECTION_BACKEND", "inmemory");
    let config = Config {
        backend: parse_backend(),
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
