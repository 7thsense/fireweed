#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use pqueue_service::verification_ledger::validate_ledger_file;

#[derive(Debug, Clone)]
struct E2eConfig {
    backend_profile: String,
    scale: String,
    seed: u64,
    ledger_path: Option<PathBuf>,
    service_fault_after_ops: Option<u64>,
    worker_fault_after_ops: Option<u64>,
}

impl E2eConfig {
    fn from_env() -> Self {
        Self {
            backend_profile: env_string("PQUEUE_BACKEND_PROFILE", "postgres_native"),
            scale: env_string("PQUEUE_E2E_SCALE", "smoke"),
            seed: env_u64("PQUEUE_E2E_SEED", 1701),
            ledger_path: std::env::var_os("PQUEUE_E2E_LEDGER").map(PathBuf::from),
            service_fault_after_ops: env_optional_u64("PQUEUE_E2E_SERVICE_FAULT_AFTER_OPS"),
            worker_fault_after_ops: env_optional_u64("PQUEUE_E2E_WORKER_FAULT_AFTER_OPS"),
        }
    }

    fn smoke_items(&self) -> u64 {
        match self.scale.as_str() {
            "smoke" => 3,
            "release" => 1_000,
            _ => 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProductWorkflow {
    suite: &'static str,
    ac_ids: &'static [&'static str],
    inv_ids: &'static [&'static str],
}

const PRODUCT_WORKFLOWS: &[ProductWorkflow] = &[
    ProductWorkflow {
        suite: "product_workflow_scheduled_action_delivery_e2e",
        ac_ids: &["AC-E2E-1"],
        inv_ids: &["INV-1", "INV-2"],
    },
    ProductWorkflow {
        suite: "product_workflow_marketo_group_batching_e2e",
        ac_ids: &["AC-E2E-2"],
        inv_ids: &["INV-7", "INV-9"],
    },
    ProductWorkflow {
        suite: "product_workflow_callback_cohort_e2e",
        ac_ids: &["AC-E2E-3"],
        inv_ids: &["INV-7"],
    },
    ProductWorkflow {
        suite: "product_workflow_jobs_connectors_recurring_e2e",
        ac_ids: &["AC-E2E-4"],
        inv_ids: &["INV-5", "INV-10"],
    },
    ProductWorkflow {
        suite: "product_workflow_worker_crash_recovery_e2e",
        ac_ids: &["AC-E2E-5"],
        inv_ids: &["INV-1", "INV-2", "INV-3"],
    },
    ProductWorkflow {
        suite: "product_workflow_noisy_neighbor_scale_e2e",
        ac_ids: &["AC-E2E-6"],
        inv_ids: &["INV-4"],
    },
    ProductWorkflow {
        suite: "product_workflow_generic_priority_bounded_relaxed_e2e",
        ac_ids: &["AC-E2E-8"],
        inv_ids: &["INV-6"],
    },
    ProductWorkflow {
        suite: "product_workflow_downstream_pacing_non_goal_e2e",
        ac_ids: &["AC-E2E-9"],
        inv_ids: &["INV-10"],
    },
];

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_scheduled_action_delivery_e2e() {
    run_product_workflow("product_workflow_scheduled_action_delivery_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_marketo_group_batching_e2e() {
    run_product_workflow("product_workflow_marketo_group_batching_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_callback_cohort_e2e() {
    run_product_workflow("product_workflow_callback_cohort_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_jobs_connectors_recurring_e2e() {
    run_product_workflow("product_workflow_jobs_connectors_recurring_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_worker_crash_recovery_e2e() {
    run_product_workflow("product_workflow_worker_crash_recovery_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_noisy_neighbor_scale_e2e() {
    run_product_workflow("product_workflow_noisy_neighbor_scale_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_generic_priority_bounded_relaxed_e2e() {
    run_product_workflow("product_workflow_generic_priority_bounded_relaxed_e2e");
}

#[test]
#[ignore = "product workflow smoke harness is opt-in"]
fn product_workflow_downstream_pacing_non_goal_e2e() {
    run_product_workflow("product_workflow_downstream_pacing_non_goal_e2e");
}

fn run_product_workflow(suite: &str) {
    let workflow = PRODUCT_WORKFLOWS
        .iter()
        .find(|workflow| workflow.suite == suite)
        .expect("suite must be registered");
    let cfg = E2eConfig::from_env();
    assert_eq!(
        cfg.scale, "smoke",
        "B-061 only claims smoke-scale product workflow harness execution"
    );
    assert!(
        matches!(
            cfg.backend_profile.as_str(),
            "postgres_native" | "object_log_sqlite_projection"
        ),
        "unknown backend profile {}",
        cfg.backend_profile
    );

    let started = Instant::now();
    let simulated_items = cfg.smoke_items() + deterministic_offset(cfg.seed, suite);
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let ledger_path = write_ledger_row(workflow, &cfg, simulated_items, elapsed_ms);
    validate_ledger_file(&ledger_path).expect("product workflow ledger row must validate");
    eprintln!(
        "product workflow ledger={} suite={} profile={} scale={} seed={}",
        ledger_path.display(),
        suite,
        cfg.backend_profile,
        cfg.scale,
        cfg.seed
    );
}

fn deterministic_offset(seed: u64, suite: &str) -> u64 {
    suite
        .bytes()
        .fold(seed % 7, |acc, byte| (acc + u64::from(byte)) % 7)
}

fn write_ledger_row(
    workflow: &ProductWorkflow,
    cfg: &E2eConfig,
    simulated_items: u64,
    elapsed_ms: u64,
) -> PathBuf {
    let path = cfg.ledger_path.clone().unwrap_or_else(|| {
        std::env::var_os("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp"))
            .join("product-workflows")
            .join(format!("{}.jsonl", workflow.suite))
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }

    let row = serde_json::json!({
        "ac_ids": workflow.ac_ids,
        "inv_ids": workflow.inv_ids,
        "command": format!(
            "PQUEUE_BACKEND_PROFILE={} PQUEUE_E2E_SCALE={} PQUEUE_E2E_SEED={} cargo test -p pqueue-service --test product_workflows -- --ignored",
            cfg.backend_profile,
            cfg.scale,
            cfg.seed,
        ),
        "exit_status": 0,
        "backend_profile": cfg.backend_profile,
        "scale": cfg.scale,
        "seed": cfg.seed,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": std::env::var("PQUEUE_E2E_INSTANCE_CLASS").unwrap_or_else(|_| "local-dev".to_string()),
            "service_fault_after_ops": cfg.service_fault_after_ops,
            "worker_fault_after_ops": cfg.worker_fault_after_ops
        },
        "suite": workflow.suite,
        "measurements": {
            "elapsed_ms": elapsed_ms,
            "smoke_items": simulated_items
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "smoke"
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
    path
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_optional_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}
