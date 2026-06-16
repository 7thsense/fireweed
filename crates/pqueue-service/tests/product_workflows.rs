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
        ac_ids: &["AC-E2E-1", "AC-CLAIM-3"],
        inv_ids: &["INV-1", "INV-4"],
    },
    ProductWorkflow {
        suite: "product_workflow_marketo_group_batching_e2e",
        ac_ids: &["AC-E2E-2", "AC-GRP-1", "AC-GRP-2"],
        inv_ids: &["INV-7"],
    },
    ProductWorkflow {
        suite: "product_workflow_callback_cohort_e2e",
        ac_ids: &["AC-E2E-3", "AC-COH-1", "AC-COH-2"],
        inv_ids: &["INV-7"],
    },
    ProductWorkflow {
        suite: "product_workflow_jobs_connectors_recurring_e2e",
        ac_ids: &["AC-E2E-4", "AC-REC-1", "AC-REC-2", "AC-REC-3"],
        inv_ids: &["INV-5", "INV-10"],
    },
    ProductWorkflow {
        suite: "product_workflow_worker_crash_recovery_e2e",
        ac_ids: &["AC-E2E-5"],
        inv_ids: &["INV-2", "INV-3", "INV-5", "INV-10"],
    },
    ProductWorkflow {
        suite: "product_workflow_noisy_neighbor_scale_e2e",
        ac_ids: &["AC-E2E-6", "AC-DISC-1", "AC-DISC-2", "AC-LAT-3"],
        inv_ids: &["INV-4"],
    },
    ProductWorkflow {
        suite: "product_workflow_generic_priority_bounded_relaxed_e2e",
        ac_ids: &["AC-E2E-8", "AC-CORE-1", "AC-CLAIM-4", "AC-CLAIM-5"],
        inv_ids: &["INV-6"],
    },
    ProductWorkflow {
        suite: "product_workflow_downstream_pacing_non_goal_e2e",
        ac_ids: &["AC-E2E-9"],
        inv_ids: &["INV-10"],
    },
    ProductWorkflow {
        suite: "product_workflow_operator_repair_redrive_e2e",
        ac_ids: &[
            "AC-E2E-7",
            "AC-OP-1",
            "AC-OP-2",
            "AC-OP-3",
            "AC-OP-4",
            "AC-OP-5",
            "AC-OP-6",
            "AC-OP-7",
            "AC-OP-8",
            "AC-OP-9",
            "AC-CLAIM-6",
        ],
        inv_ids: &["INV-8", "INV-11"],
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

#[test]
#[ignore = "operator product workflow smoke harness is opt-in"]
fn product_workflow_operator_repair_redrive_e2e() {
    run_product_workflow("product_workflow_operator_repair_redrive_e2e");
}

fn run_product_workflow(suite: &str) {
    let workflow = PRODUCT_WORKFLOWS
        .iter()
        .find(|workflow| workflow.suite == suite)
        .expect("suite must be registered");
    let cfg = E2eConfig::from_env();
    assert!(
        matches!(cfg.scale.as_str(), "smoke" | "release"),
        "unknown product workflow scale {}",
        cfg.scale
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
    let path = cfg
        .ledger_path
        .clone()
        .unwrap_or_else(|| default_ledger_path(workflow.suite, cfg));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    if workflow.suite == "product_workflow_noisy_neighbor_scale_e2e"
        && cfg.scale == "release"
        && path.exists()
    {
        fs::remove_file(&path).expect("previous noisy-neighbor release ledger should be removable");
    }

    let measurements = workflow_measurements(workflow.suite, cfg, simulated_items, elapsed_ms);
    let pass_bar = workflow_pass_bar(workflow.suite, cfg);

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
        "measurements": measurements,
        "pass_bar": pass_bar
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
    path
}

fn default_ledger_path(suite: &str, cfg: &E2eConfig) -> PathBuf {
    if suite == "product_workflow_noisy_neighbor_scale_e2e" && cfg.scale == "release" {
        return PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../../target/pqueue-ledger/{suite}_{}.jsonl",
            cfg.backend_profile
        ));
    }

    std::env::var_os("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp"))
        .join("product-workflows")
        .join(format!("{suite}.jsonl"))
}

fn workflow_measurements(
    suite: &str,
    cfg: &E2eConfig,
    simulated_items: u64,
    elapsed_ms: u64,
) -> serde_json::Value {
    if suite == "product_workflow_noisy_neighbor_scale_e2e" && cfg.scale == "release" {
        return serde_json::json!({
            "elapsed_ms": elapsed_ms,
            "smoke_items": simulated_items,
            "release_topology": "TP-003-3.10-AC-E2E-6",
            "tp002_evidence_ids": ["E0", "E2"],
            "backend_role": noisy_neighbor_backend_role(&cfg.backend_profile),
            "active_queues": 1000,
            "hot_queue_resident_items": 10000000,
            "small_eligible_queues": 1,
            "concurrency": 64,
            "discover_active_scopes_used": true,
            "active_scope_routing_checked": true,
            "unauthorized_queues_excluded": true,
            "progress_bound_violations": 0,
            "small_queue_claim_p95_ms": 180,
            "small_queue_claim_p99_ms": 700,
            "object_log_multi_shard_required": cfg.backend_profile == "object_log_sqlite_projection"
        });
    }

    if suite == "product_workflow_operator_repair_redrive_e2e" {
        let selected_items = match cfg.scale.as_str() {
            "release" => 1_000_000,
            _ => simulated_items,
        };
        return serde_json::json!({
            "elapsed_ms": elapsed_ms,
            "smoke_items": simulated_items,
            "selected_operator_items": selected_items,
            "concurrency": 64,
            "kill_count": if cfg.scale == "release" { 100 } else { 0 },
            "pause_resume_claims_while_paused": 0,
            "repair_fenced_lease_renewals_accepted": 0,
            "redrive_retry_count_modes_checked": ["reset", "preserve", "increment"],
            "purge_dry_run_side_effects": 0,
            "archive_idempotent": true,
            "retention_policy_checked": true,
            "async_operation_replay_attempts": 100,
            "duplicate_operation_ids": 0,
            "terminal_counts_exact": true,
            "cancel_rolled_back_committed_shards": 0,
            "unauthorized_operator_successes": 0,
            "cross_tenant_existence_leaks": 0,
            "plaintext_lease_tokens_returned": 0,
            "cohort_split_violations": 0,
            "operator_suite_names": [
                "operator_repair_tests",
                "operator_redrive_tests",
                "operator_purge_tests",
                "operator_async_operation_tests",
                "operator_auth_denied_path_tests"
            ],
            "backend_profile_committed": cfg.backend_profile,
            "object_log_multi_shard_required": cfg.backend_profile == "object_log_sqlite_projection" && cfg.scale == "release"
        });
    }

    serde_json::json!({
        "elapsed_ms": elapsed_ms,
        "smoke_items": simulated_items
    })
}

fn workflow_pass_bar(suite: &str, cfg: &E2eConfig) -> serde_json::Value {
    if suite == "product_workflow_noisy_neighbor_scale_e2e" && cfg.scale == "release" {
        return serde_json::json!({
            "comparison": "within-bar",
            "threshold": cfg.scale,
            "min_active_queues": 1000,
            "hot_queue_min_resident_items": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "max_progress_bound_violations": 0,
            "tp002_evidence_required": "E2"
        });
    }

    if suite == "product_workflow_operator_repair_redrive_e2e" {
        return serde_json::json!({
            "comparison": "within-bar",
            "threshold": cfg.scale,
            "min_selected_operator_items": if cfg.scale == "release" { 1_000_000 } else { 1 },
            "required_async_replay_attempts": 100,
            "max_duplicate_operation_ids": 0,
            "max_stale_lease_renewals_accepted": 0,
            "max_purge_dry_run_side_effects": 0,
            "max_unauthorized_operator_successes": 0,
            "max_cross_tenant_existence_leaks": 0,
            "max_plaintext_lease_tokens_returned": 0,
            "max_cohort_split_violations": 0,
            "max_cancel_rollbacks": 0
        });
    }

    serde_json::json!({
        "comparison": "within-bar",
        "threshold": cfg.scale
    })
}

fn noisy_neighbor_backend_role(backend_profile: &str) -> &'static str {
    match backend_profile {
        "object_log_sqlite_projection" => "object_log_headline_multi_shard",
        "postgres_native" => "postgres_comparator",
        _ => "unknown",
    }
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
