use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::performance_matrix::{OperationSamples, RepetitionResult};
use crate::performance_matrix_analysis::{Comparison, build_comparisons};
use crate::performance_matrix_lifecycle::{ProjectionMaintenanceResult, RecoveryResult};
use crate::performance_matrix_provenance::Provenance;

pub const SCHEMA_VERSION: &str = "fireweed-performance-matrix-v1";

/// TP-005 full register: exactly 20 `log--projection` cells. Barrier class is Strict.
const FULL_CELLS: &[(&str, &str)] = &[
    ("memory--memory", "Strict"),
    ("memory--sqlite", "Strict"),
    ("memory--turso", "Strict"),
    ("memory--postgres", "Strict"),
    ("sqlite--memory", "Strict"),
    ("sqlite--sqlite", "Strict"),
    ("sqlite--turso", "Strict"),
    ("sqlite--postgres", "Strict"),
    ("postgres--memory", "Strict"),
    ("postgres--sqlite", "Strict"),
    ("postgres--turso", "Strict"),
    ("postgres--postgres", "Strict"),
    ("filesystem--memory", "Strict"),
    ("filesystem--sqlite", "Strict"),
    ("filesystem--turso", "Strict"),
    ("filesystem--postgres", "Strict"),
    ("s3--memory", "Strict"),
    ("s3--sqlite", "Strict"),
    ("s3--turso", "Strict"),
    ("s3--postgres", "Strict"),
];

/// Smoke: local logs × local projections (9 cells). No live PG/S3 required.
const SMOKE_CELLS: &[(&str, &str)] = &[
    ("memory--memory", "Strict"),
    ("memory--sqlite", "Strict"),
    ("memory--turso", "Strict"),
    ("sqlite--memory", "Strict"),
    ("sqlite--sqlite", "Strict"),
    ("sqlite--turso", "Strict"),
    ("filesystem--memory", "Strict"),
    ("filesystem--sqlite", "Strict"),
    ("filesystem--turso", "Strict"),
];

const FULL_SHAPES: &[(&str, u64, usize)] = &[
    ("minimal", 12_800, 128),
    ("record-1k", 12_800, 128),
    ("group-keyed-256", 12_800, 128),
    ("large-16k", 1_600, 16),
];

const SMOKE_SHAPES: &[(&str, u64, usize)] = &[("minimal", 512, 64)];

pub fn build_schedule(tier: &str) -> Result<Vec<ScheduleEntry>, String> {
    let (cells, shapes, repetitions) = match tier {
        "full" => (FULL_CELLS, FULL_SHAPES, 5),
        "smoke" => (SMOKE_CELLS, SMOKE_SHAPES, 1),
        _ => return Err("unsupported schedule tier".into()),
    };
    let mut schedule = Vec::new();
    let mut push = |phase: &str, repetition: usize, shape: &str, cell: &str| {
        schedule.push(ScheduleEntry {
            ordinal: schedule.len(),
            phase: phase.into(),
            repetition,
            shape: shape.into(),
            cell: cell.into(),
        });
    };
    for (shape, _, _) in shapes {
        for (cell, _) in cells {
            push("warmup", 0, shape, cell);
        }
    }
    for repetition in 0..repetitions {
        for (shape_index, (shape, _, _)) in shapes.iter().enumerate() {
            let mut ordered = cells.iter().map(|(cell, _)| *cell).collect::<Vec<_>>();
            let count = ordered.len();
            ordered.rotate_left((repetition + shape_index) % count);
            if repetition % 2 == 1 {
                ordered.reverse();
            }
            for cell in ordered {
                push("common", repetition, shape, cell);
            }
        }
    }
    if tier == "full" {
        for shape in ["minimal", "record-1k"] {
            // Recovery on every durable-log cell (Class A); Class B memory-log rows still
            // run reopen with projection-boundary semantics (include all non memory--memory).
            for (cell, _) in FULL_CELLS
                .iter()
                .filter(|(cell, _)| *cell != "memory--memory")
            {
                for repetition in 0..3 {
                    push("recovery", repetition, shape, cell);
                }
            }
        }
        // Maintenance: disposable projection rebuild for filesystem/s3 × non-memory projection.
        for (cell, _) in FULL_CELLS.iter().filter(|(cell, _)| {
            let Some((log, proj)) = cell.split_once("--") else {
                return false;
            };
            matches!(log, "filesystem" | "s3") && proj != "memory"
        }) {
            for repetition in 0..3 {
                push("maintenance", repetition, "record-1k", cell);
            }
        }
    }
    Ok(schedule)
}

pub fn validate_checkpoint_row(tier: &str, row: &RepetitionResult) -> Result<(), String> {
    let (cells, shapes, repetitions) = match tier {
        "full" => (FULL_CELLS, FULL_SHAPES, 5),
        "smoke" => (SMOKE_CELLS, SMOKE_SHAPES, 1),
        _ => return Err("unsupported checkpoint tier".into()),
    };
    if !cells.iter().any(|(cell, _)| *cell == row.cell) || row.repetition >= repetitions {
        return Err("checkpoint row has an invalid cell or repetition".into());
    }
    let (_, items, batch) = shapes
        .iter()
        .find(|(shape, _, _)| *shape == row.shape)
        .ok_or_else(|| "checkpoint row has an invalid shape".to_owned())?;
    let samples = *items as usize / *batch;
    if row.items != *items
        || row.batch != *batch
        || row.accepted != *items
        || row.claimed != *items
        || row.finalized != *items
        || [
            (&row.append, "append"),
            (&row.claim, "claim"),
            (&row.finalize, "finalize"),
        ]
        .iter()
        .any(|(operation, name)| {
            operation.operation != *name
                || operation.items != *items
                || operation.durations_ns.len() != samples
                || operation.total_ns == 0
        })
        || row.projection_catchup.is_some()
    // baseline Strict matrix has no async catch-up rows
    {
        return Err("checkpoint row violates matrix workload invariants".into());
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixEvidence {
    pub schema_version: String,
    pub run_id: String,
    pub tier: String,
    pub status: String,
    pub command: Vec<String>,
    pub seed: u64,
    pub resolved_config_sha256: String,
    pub schedule: Vec<ScheduleEntry>,
    pub unsupported_cells: Vec<String>,
    pub git_commit: String,
    pub git_branch: String,
    pub host_fingerprint_sha256: String,
    pub provenance: Provenance,
    pub source_clean: bool,
    pub submodule_status: String,
    pub enabled_features: String,
    pub rustflags_sha256: String,
    pub service_topology: ServiceTopology,
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub shapes: Vec<ShapeEvidence>,
    pub cells: Vec<CellEvidence>,
    pub repetitions: Vec<RepetitionResult>,
    pub summaries: Vec<Summary>,
    pub comparisons: Vec<Comparison>,
    pub recovery: Vec<RecoveryResult>,
    pub maintenance: Vec<ProjectionMaintenanceResult>,
    pub cleanup: Vec<CleanupEvidence>,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleEntry {
    pub ordinal: usize,
    pub phase: String,
    pub repetition: usize,
    pub shape: String,
    pub cell: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceTopology {
    pub postgres_configured: bool,
    pub postgres_server: Option<String>,
    pub postgres_durability: Option<String>,
    pub object_store_configured: bool,
    pub object_store_scheme: Option<String>,
    pub object_store_endpoint_sha256: Option<String>,
    pub object_store_bucket_sha256: Option<String>,
    pub object_store_region: Option<String>,
    pub object_store_provider: Option<String>,
    pub object_store_preflight_rtt_ns: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupEvidence {
    pub phase: String,
    pub cell: String,
    pub shape: String,
    pub repetition: usize,
    pub logical_namespace: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShapeEvidence {
    pub id: String,
    pub items: u64,
    pub batch: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellEvidence {
    pub id: String,
    pub barrier_class: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Summary {
    pub cell: String,
    pub shape: String,
    pub operation: String,
    pub samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub median_items_per_second: f64,
    pub throughput_cv: f64,
}

fn percentile(samples: &mut [u64], p: f64) -> u64 {
    samples.sort_unstable();
    let rank = ((samples.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[rank]
}

fn summarize_operation(
    cell: &str,
    shape: &str,
    operation: &str,
    rows: &[&RepetitionResult],
    select: impl Fn(&RepetitionResult) -> &OperationSamples,
) -> Summary {
    let mut samples = rows
        .iter()
        .flat_map(|row| select(row).durations_ns.iter().copied())
        .collect::<Vec<_>>();
    let mut throughputs = rows
        .iter()
        .map(|row| {
            let op = select(row);
            op.items as f64 / (op.total_ns as f64 / 1_000_000_000.0)
        })
        .collect::<Vec<_>>();
    throughputs.sort_by(f64::total_cmp);
    let mean = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
    let variance = throughputs
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / throughputs.len() as f64;
    Summary {
        cell: cell.into(),
        shape: shape.into(),
        operation: operation.into(),
        samples: samples.len(),
        p50_ns: percentile(&mut samples.clone(), 0.50),
        p95_ns: percentile(&mut samples.clone(), 0.95),
        p99_ns: percentile(&mut samples, 0.99),
        median_items_per_second: throughputs[throughputs.len() / 2],
        throughput_cv: variance.sqrt() / mean,
    }
}

pub fn build_summaries(rows: &[RepetitionResult]) -> Vec<Summary> {
    let mut keys = rows
        .iter()
        .map(|row| (row.cell.clone(), row.shape.clone()))
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let mut summaries = Vec::new();
    for (cell, shape) in keys {
        let group = rows
            .iter()
            .filter(|row| row.cell == cell && row.shape == shape)
            .collect::<Vec<_>>();
        summaries.push(summarize_operation(&cell, &shape, "append", &group, |r| {
            &r.append
        }));
        summaries.push(summarize_operation(&cell, &shape, "claim", &group, |r| {
            &r.claim
        }));
        summaries.push(summarize_operation(
            &cell,
            &shape,
            "finalize",
            &group,
            |r| &r.finalize,
        ));
    }
    summaries
}

pub fn canonical_bytes(evidence: &MatrixEvidence) -> Result<Vec<u8>, String> {
    let mut bytes = serde_json::to_vec_pretty(evidence).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn write_evidence(
    path: &fireweed_release::RunOwned,
    digest_path: &fireweed_release::RunOwned,
    evidence: &MatrixEvidence,
) -> Result<(), String> {
    let bytes = canonical_bytes(evidence)?;
    path.write(&bytes).map_err(|error| error.to_string())?;
    digest_path
        .write(format!(
            "{}  {}\n",
            digest_hex(&bytes),
            path.path().file_name().unwrap().to_string_lossy()
        ))
        .map_err(|error| error.to_string())
}

fn verify_evidence(evidence: &MatrixEvidence) -> Result<(), String> {
    if evidence.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported schema version {}",
            evidence.schema_version
        ));
    }
    if evidence.status != "passed" {
        return Err(format!(
            "matrix status is {:?}, expected \"passed\"",
            evidence.status
        ));
    }
    if !evidence.failures.is_empty() {
        return Err("passed matrix contains failures".into());
    }
    if evidence.seed != 0x5eed_f17e_0eed
        || evidence.resolved_config_sha256.len() != 64
        || !evidence
            .resolved_config_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || evidence.schedule != build_schedule(&evidence.tier)?
        || !evidence.unsupported_cells.is_empty()
        || evidence.command.iter().any(|argument| {
            let lower = argument.to_ascii_lowercase();
            lower.contains("password=")
                || lower.contains("secret=")
                || lower.contains("access_key=")
        })
    {
        return Err(
            "profile seed, configuration, schedule, manifest, or command is invalid".into(),
        );
    }
    if evidence.git_commit.len() != 40
        || !evidence
            .git_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || evidence.provenance.host_fingerprint_sha256 != evidence.host_fingerprint_sha256
        || evidence.provenance.logical_cpu_count == 0
        || evidence.provenance.total_memory_kib == 0
    {
        return Err("source or host provenance is incomplete or inconsistent".into());
    }

    let (expected_cells, expected_shapes, expected_repetitions) = match evidence.tier.as_str() {
        "full" => (FULL_CELLS, FULL_SHAPES, 5),
        "smoke" => (SMOKE_CELLS, SMOKE_SHAPES, 1),
        other => return Err(format!("unsupported matrix tier {other:?}")),
    };
    if evidence.tier == "full" && evidence.provenance.remote_commit != evidence.git_commit {
        return Err("full matrix source commit does not match fetched origin/main".into());
    }
    if evidence.tier == "full"
        && (!evidence.source_clean
            || !evidence.enabled_features.contains("profile=release")
            || evidence.rustflags_sha256.len() != 64
            || !evidence.service_topology.postgres_configured
            || evidence.service_topology.postgres_server.is_none()
            || evidence.service_topology.postgres_durability.is_none()
            || !evidence.service_topology.object_store_configured
            || evidence.service_topology.object_store_scheme.is_none()
            || evidence
                .service_topology
                .object_store_endpoint_sha256
                .as_ref()
                .is_none_or(|value| value.len() != 64)
            || evidence
                .service_topology
                .object_store_bucket_sha256
                .as_ref()
                .is_none_or(|value| value.len() != 64)
            || evidence.service_topology.object_store_region.is_none()
            || evidence
                .service_topology
                .object_store_provider
                .as_ref()
                .is_none_or(|p| p.is_empty())
            || evidence
                .service_topology
                .object_store_preflight_rtt_ns
                .len()
                != 3)
    {
        return Err("full matrix source, build, or service provenance is incomplete".into());
    }
    if evidence.tier == "full"
        && [
            evidence.provenance.conformance_output_sha256.as_deref(),
            evidence.provenance.benchmark_test_output_sha256.as_deref(),
        ]
        .iter()
        .any(|value| {
            value.is_none_or(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        })
    {
        return Err("full matrix same-commit preflight digests are missing or invalid".into());
    }

    let expected_cell_map = expected_cells.iter().copied().collect::<BTreeMap<_, _>>();
    let mut actual_cell_ids = BTreeSet::new();
    for cell in &evidence.cells {
        if !actual_cell_ids.insert(cell.id.as_str()) {
            return Err(format!("duplicate cell metadata for {}", cell.id));
        }
        let expected_barrier = expected_cell_map
            .get(cell.id.as_str())
            .ok_or_else(|| format!("unexpected cell metadata for {}", cell.id))?;
        if cell.status != "passed" {
            return Err(format!("cell {} status is not passed", cell.id));
        }
        if cell.barrier_class != *expected_barrier {
            return Err(format!(
                "cell {} has barrier class {}, expected {}",
                cell.id, cell.barrier_class, expected_barrier
            ));
        }
    }
    let expected_cell_ids = expected_cell_map.keys().copied().collect::<BTreeSet<_>>();
    if actual_cell_ids != expected_cell_ids {
        let missing = expected_cell_ids
            .difference(&actual_cell_ids)
            .copied()
            .collect::<Vec<_>>();
        return Err(format!(
            "cell metadata set is incomplete; missing {missing:?}"
        ));
    }

    let expected_shape_map = expected_shapes
        .iter()
        .map(|(id, items, batch)| (*id, (*items, *batch)))
        .collect::<BTreeMap<_, _>>();
    let mut actual_shape_ids = BTreeSet::new();
    for shape in &evidence.shapes {
        if !actual_shape_ids.insert(shape.id.as_str()) {
            return Err(format!("duplicate shape metadata for {}", shape.id));
        }
        let expected = expected_shape_map
            .get(shape.id.as_str())
            .ok_or_else(|| format!("unexpected shape metadata for {}", shape.id))?;
        if (shape.items, shape.batch) != *expected {
            return Err(format!(
                "shape {} has items/batch {}/{}, expected {}/{}",
                shape.id, shape.items, shape.batch, expected.0, expected.1
            ));
        }
    }
    let expected_shape_ids = expected_shape_map.keys().copied().collect::<BTreeSet<_>>();
    if actual_shape_ids != expected_shape_ids {
        let missing = expected_shape_ids
            .difference(&actual_shape_ids)
            .copied()
            .collect::<Vec<_>>();
        return Err(format!(
            "shape metadata set is incomplete; missing {missing:?}"
        ));
    }

    let expected_row_count = expected_cells.len() * expected_shapes.len() * expected_repetitions;
    if evidence.repetitions.len() != expected_row_count {
        return Err(format!(
            "matrix has {} repetition rows, expected {expected_row_count}",
            evidence.repetitions.len()
        ));
    }
    let mut tuples = BTreeSet::new();
    for row in &evidence.repetitions {
        let (expected_items, expected_batch) = expected_shape_map
            .get(row.shape.as_str())
            .copied()
            .ok_or_else(|| format!("row references unexpected shape {}", row.shape))?;
        if !expected_cell_map.contains_key(row.cell.as_str()) {
            return Err(format!("row references unexpected cell {}", row.cell));
        }
        if row.repetition >= expected_repetitions {
            return Err(format!(
                "{}/{} has out-of-range repetition {}",
                row.cell, row.shape, row.repetition
            ));
        }
        if !tuples.insert((row.cell.as_str(), row.shape.as_str(), row.repetition)) {
            return Err(format!(
                "duplicate repetition tuple {}/{} r{}",
                row.cell, row.shape, row.repetition
            ));
        }
        if (row.items, row.batch) != (expected_items, expected_batch) {
            return Err(format!(
                "{} {} has items/batch {}/{}, expected {expected_items}/{expected_batch}",
                row.cell, row.shape, row.items, row.batch
            ));
        }
        let expected_samples = expected_items as usize / expected_batch;
        for (op, expected_name) in [
            (&row.append, "append"),
            (&row.claim, "claim"),
            (&row.finalize, "finalize"),
        ] {
            if op.operation != expected_name {
                return Err(format!(
                    "{} {}/{} operation is {}, expected {expected_name}",
                    row.cell, row.shape, row.repetition, op.operation
                ));
            }
            if op.items != expected_items {
                return Err(format!(
                    "{} {}/{} {} records {} items, expected {expected_items}",
                    row.cell, row.shape, row.repetition, op.operation, op.items
                ));
            }
            if op.durations_ns.len() != expected_samples {
                return Err(format!(
                    "{} {}/{} has {} samples, expected {expected_samples}",
                    row.cell,
                    row.shape,
                    op.operation,
                    op.durations_ns.len()
                ));
            }
        }
        if row.accepted != expected_items
            || row.claimed != expected_items
            || row.finalized != expected_items
        {
            return Err(format!("{} {} does not reconcile", row.cell, row.shape));
        }
        // Baseline TP-005 Strict matrix has no async catch-up rows.
        if row.projection_catchup.is_some() {
            return Err(format!(
                "{} {} has unexpected async catch-up evidence",
                row.cell, row.shape
            ));
        }
    }

    let expected_common_order = evidence
        .schedule
        .iter()
        .filter(|entry| entry.phase == "common")
        .map(|entry| (entry.cell.as_str(), entry.shape.as_str(), entry.repetition))
        .collect::<Vec<_>>();
    let actual_common_order = evidence
        .repetitions
        .iter()
        .map(|row| (row.cell.as_str(), row.shape.as_str(), row.repetition))
        .collect::<Vec<_>>();
    if actual_common_order != expected_common_order {
        return Err("repetition rows do not follow the declared measured schedule".into());
    }

    for &(cell, _) in expected_cells {
        for &(shape, _, _) in expected_shapes {
            for repetition in 0..expected_repetitions {
                if !tuples.contains(&(cell, shape, repetition)) {
                    return Err(format!(
                        "missing repetition tuple {cell}/{shape} r{repetition}"
                    ));
                }
            }
        }
    }

    let recomputed = build_summaries(&evidence.repetitions);
    let summaries_match = recomputed.len() == evidence.summaries.len()
        && recomputed
            .iter()
            .zip(&evidence.summaries)
            .all(|(left, right)| {
                left.cell == right.cell
                    && left.shape == right.shape
                    && left.operation == right.operation
                    && left.samples == right.samples
                    && left.p50_ns == right.p50_ns
                    && left.p95_ns == right.p95_ns
                    && left.p99_ns == right.p99_ns
                    && (left.median_items_per_second - right.median_items_per_second).abs()
                        <= left.median_items_per_second.abs().max(1.0) * 1e-12
                    && (left.throughput_cv - right.throughput_cv).abs()
                        <= left.throughput_cv.abs().max(1.0) * 1e-12
            });
    if !summaries_match {
        return Err("summary mismatch".into());
    }
    let expected_comparisons = build_comparisons(&evidence.repetitions, &recomputed);
    let comparisons_match = expected_comparisons.len() == evidence.comparisons.len()
        && expected_comparisons
            .iter()
            .zip(&evidence.comparisons)
            .all(|(left, right)| {
                left.left == right.left
                    && left.right == right.right
                    && left.shape == right.shape
                    && left.operation == right.operation
                    && left.status == right.status
                    && left.rounds_left_faster == right.rounds_left_faster
                    && left.rounds_right_faster == right.rounds_right_faster
                    && match (left.median_throughput_ratio, right.median_throughput_ratio) {
                        (None, None) => true,
                        (Some(left), Some(right)) => {
                            (left - right).abs() <= left.abs().max(1.0) * 1e-12
                        }
                        _ => false,
                    }
            });
    if !comparisons_match {
        return Err("comparison mismatch".into());
    }

    let common_cleanup = evidence
        .cleanup
        .iter()
        .filter(|cleanup| cleanup.phase == "common")
        .collect::<Vec<_>>();
    if common_cleanup.len() != expected_row_count {
        return Err(format!(
            "matrix has {} common cleanup records, expected {expected_row_count}",
            common_cleanup.len()
        ));
    }
    let mut cleanup_tuples = BTreeSet::new();
    for cleanup in common_cleanup {
        if cleanup.status != "passed" {
            return Err("common cleanup record is not passed".into());
        }
        if !tuples.contains(&(
            cleanup.cell.as_str(),
            cleanup.shape.as_str(),
            cleanup.repetition,
        )) || !cleanup_tuples.insert((
            cleanup.cell.as_str(),
            cleanup.shape.as_str(),
            cleanup.repetition,
        )) {
            return Err("cleanup record does not map one-to-one to a matrix row".into());
        }
        let expected_namespace = format!(
            "fireweed-perf/v1/{}/{}/{}/{}/r{:02}",
            &evidence.git_commit[..12],
            evidence.run_id,
            cleanup.cell,
            cleanup.shape,
            cleanup.repetition
        );
        if cleanup.logical_namespace != expected_namespace {
            return Err("cleanup logical namespace does not match its matrix row".into());
        }
    }
    if evidence.tier == "smoke" {
        if !evidence.recovery.is_empty()
            || !evidence.maintenance.is_empty()
            || evidence.cleanup.len() != expected_row_count
        {
            return Err("smoke evidence must not claim lifecycle results or cleanup".into());
        }
    } else {
        let recovery_cells = FULL_CELLS
            .iter()
            .map(|(cell, _)| *cell)
            .filter(|cell| *cell != "memory--memory")
            .collect::<BTreeSet<_>>();
        let recovery_shapes = [("minimal", 12_800, 128), ("record-1k", 12_800, 128)];
        let expected_recovery = recovery_cells.len() * recovery_shapes.len() * 3;
        if evidence.recovery.len() != expected_recovery {
            return Err(format!(
                "matrix has {} recovery results, expected {expected_recovery}",
                evidence.recovery.len()
            ));
        }
        let mut recovery_tuples = BTreeSet::new();
        for result in &evidence.recovery {
            let (_, expected_items, expected_batch) = recovery_shapes
                .iter()
                .find(|(shape, _, _)| *shape == result.population.shape)
                .ok_or_else(|| "recovery result has an unexpected shape".to_owned())?;
            if !recovery_cells.contains(result.cell.as_str())
                || result.repetition >= 3
                || !recovery_tuples.insert((
                    result.cell.as_str(),
                    result.population.shape.as_str(),
                    result.repetition,
                ))
                || result.population.items != *expected_items
                || result.population.batch != *expected_batch
                || result.population.identity_sha256.len() != 64
                || result.population.content_sha256.len() != 64
                || result.population.metrics.pending != *expected_items
                || result.population.metrics.leased != 0
                || result.reopened_metrics.pending != *expected_items
                || result.reopened_metrics.leased != 0
                || result.drained_metrics.pending != 0
                || result.drained_metrics.leased != 0
                || result.drained_content_sha256 != result.population.content_sha256
            {
                return Err("recovery result is incomplete, duplicated, or inconsistent".into());
            }
        }
        let maintenance_cells = FULL_CELLS
            .iter()
            .map(|(cell, _)| *cell)
            .filter(|cell| {
                cell.split_once("--").is_some_and(|(log, proj)| {
                    matches!(log, "filesystem" | "s3") && proj != "memory"
                })
            })
            .collect::<BTreeSet<_>>();
        let expected_maintenance = maintenance_cells.len() * 3;
        if evidence.maintenance.len() != expected_maintenance {
            return Err(format!(
                "matrix has {} maintenance results, expected {expected_maintenance}",
                evidence.maintenance.len()
            ));
        }
        let mut maintenance_tuples = BTreeSet::new();
        for result in &evidence.maintenance {
            if !maintenance_cells.contains(result.cell.as_str())
                || result.repetition >= 3
                || !maintenance_tuples.insert((result.cell.as_str(), result.repetition))
                || result.population.shape != "record-1k"
                || result.population.items != 12_800
                || result.population.batch != 128
                || !(result.capabilities.verify
                    && result.capabilities.delete
                    && result.capabilities.rebuild)
                || !result.verification_before.compatible
                || result.verification_before.projection_sequence
                    != result.verification_before.authoritative_sequence
                || !result.verification_after.compatible
                || result.verification_after.projection_sequence
                    != result.verification_after.authoritative_sequence
                || result.rebuild.projection_sequence
                    != result.verification_after.projection_sequence
                || result.population.identity_sha256 != result.post_rebuild_identity_sha256
                || result.drained_content_sha256 != result.population.content_sha256
                || result.post_rebuild_metrics.pending != 12_800
                || result.post_rebuild_metrics.leased != 0
                || result.drained_metrics.pending != 0
                || result.drained_metrics.leased != 0
            {
                return Err("maintenance result is incomplete, duplicated, or inconsistent".into());
            }
        }

        if evidence.cleanup.len() != expected_row_count + expected_recovery + expected_maintenance {
            return Err("full matrix cleanup record count is incomplete".into());
        }
        let mut lifecycle_cleanup = BTreeSet::new();
        for cleanup in evidence
            .cleanup
            .iter()
            .filter(|value| value.phase != "common")
        {
            let coordinate_shape = format!("{}-{}", cleanup.phase, cleanup.shape);
            let expected_namespace = format!(
                "fireweed-perf/v1/{}/{}/{}/{}/r{:02}",
                &evidence.git_commit[..12],
                evidence.run_id,
                cleanup.cell,
                coordinate_shape,
                cleanup.repetition
            );
            let present = match cleanup.phase.as_str() {
                "recovery" => recovery_tuples.contains(&(
                    cleanup.cell.as_str(),
                    cleanup.shape.as_str(),
                    cleanup.repetition,
                )),
                "maintenance" => {
                    maintenance_tuples.contains(&(cleanup.cell.as_str(), cleanup.repetition))
                }
                _ => false,
            };
            if cleanup.status != "passed"
                || !present
                || !lifecycle_cleanup.insert((
                    cleanup.phase.as_str(),
                    cleanup.cell.as_str(),
                    cleanup.shape.as_str(),
                    cleanup.repetition,
                ))
                || cleanup.logical_namespace != expected_namespace
            {
                return Err("lifecycle cleanup record is invalid".into());
            }
        }
    }
    Ok(())
}

pub fn verify_file(path: &Path) -> Result<MatrixEvidence, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let evidence: MatrixEvidence =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    verify_evidence(&evidence)?;
    let sidecar = fs::read_to_string(path.with_extension("json.sha256"))
        .map_err(|error| error.to_string())?;
    if sidecar.split_whitespace().next() != Some(&digest_hex(&bytes)) {
        return Err("sidecar digest mismatch".into());
    }
    Ok(evidence)
}

#[cfg(test)]
mod tests {
    use super::*;

    type EvidenceMutation = Box<dyn Fn(&mut MatrixEvidence)>;

    fn operation(name: &str) -> OperationSamples {
        OperationSamples {
            operation: name.into(),
            durations_ns: vec![1; 8],
            total_ns: 8,
            items: 512,
        }
    }

    fn smoke_evidence() -> MatrixEvidence {
        let repetitions = SMOKE_CELLS
            .iter()
            .map(|(cell, _)| RepetitionResult {
                cell: (*cell).into(),
                shape: "minimal".into(),
                repetition: 0,
                items: 512,
                batch: 64,
                append: operation("append"),
                claim: operation("claim"),
                finalize: operation("finalize"),
                accepted: 512,
                claimed: 512,
                finalized: 512,
                projection_catchup: None,
            })
            .collect::<Vec<_>>();
        let summaries = build_summaries(&repetitions);
        MatrixEvidence {
            schema_version: SCHEMA_VERSION.into(),
            run_id: "unit".into(),
            tier: "smoke".into(),
            status: "passed".into(),
            command: vec!["--tier".into(), "smoke".into()],
            seed: 0x5eed_f17e_0eed,
            resolved_config_sha256: "0".repeat(64),
            schedule: build_schedule("smoke").unwrap(),
            unsupported_cells: vec![],
            git_commit: "0".repeat(40),
            git_branch: "main".into(),
            host_fingerprint_sha256: "0".repeat(64),
            provenance: Provenance {
                remote_ref: "refs/remotes/origin/main".into(),
                remote_commit: "0".repeat(40),
                remote_url: "https://example.test/fireweed".into(),
                os_kernel: "test".into(),
                architecture: "test".into(),
                cpu_model: "test".into(),
                logical_cpu_count: 1,
                total_memory_kib: 1,
                rustc_version: "rustc test".into(),
                cargo_version: "cargo test".into(),
                filesystem: "test".into(),
                free_space: "test".into(),
                load_average: "test".into(),
                cpu_governor: "test".into(),
                turbo_state: "test".into(),
                virtualization: "test".into(),
                host_fingerprint_sha256: "0".repeat(64),
                conformance_output_sha256: None,
                benchmark_test_output_sha256: None,
            },
            source_clean: false,
            submodule_status: String::new(),
            enabled_features: "test".into(),
            rustflags_sha256: "0".repeat(64),
            service_topology: ServiceTopology {
                postgres_configured: false,
                postgres_server: None,
                postgres_durability: None,
                object_store_configured: false,
                object_store_scheme: None,
                object_store_endpoint_sha256: None,
                object_store_bucket_sha256: None,
                object_store_region: None,
                object_store_provider: None,
                object_store_preflight_rtt_ns: Vec::new(),
            },
            started_unix_ms: 1,
            finished_unix_ms: 2,
            shapes: vec![ShapeEvidence {
                id: "minimal".into(),
                items: 512,
                batch: 64,
            }],
            cells: SMOKE_CELLS
                .iter()
                .map(|(id, barrier_class)| CellEvidence {
                    id: (*id).into(),
                    barrier_class: (*barrier_class).into(),
                    status: "passed".into(),
                })
                .collect(),
            comparisons: build_comparisons(&repetitions, &summaries),
            summaries,
            recovery: Vec::new(),
            maintenance: Vec::new(),
            cleanup: repetitions
                .iter()
                .map(|row| CleanupEvidence {
                    phase: "common".into(),
                    cell: row.cell.clone(),
                    shape: row.shape.clone(),
                    repetition: row.repetition,
                    logical_namespace: format!(
                        "fireweed-perf/v1/000000000000/unit/{}/{}/r00",
                        row.cell, row.shape
                    ),
                    status: "passed".into(),
                })
                .collect(),
            repetitions,
            failures: Vec::new(),
        }
    }

    #[test]
    fn accepts_exact_smoke_matrix() {
        verify_evidence(&smoke_evidence()).expect("complete smoke evidence should verify");
    }

    #[test]
    fn rejects_empty_evidence() {
        let mut evidence = smoke_evidence();
        evidence.cells.clear();
        evidence.shapes.clear();
        evidence.repetitions.clear();
        evidence.summaries.clear();
        assert!(verify_evidence(&evidence).is_err());
    }

    #[test]
    fn rejects_partial_matrix() {
        let mut evidence = smoke_evidence();
        evidence.repetitions.pop();
        let error = verify_evidence(&evidence).expect_err("partial matrix must fail");
        assert!(error.contains("repetition rows"), "{error}");
    }

    #[test]
    fn rejects_mutated_evidence() {
        let mutations: Vec<EvidenceMutation> = vec![
            Box::new(|evidence| evidence.status = "failed".into()),
            Box::new(|evidence| evidence.failures.push("injected".into())),
            Box::new(|evidence| evidence.cells[0].barrier_class = "wrong".into()),
            Box::new(|evidence| evidence.shapes[0].batch = 32),
            Box::new(|evidence| evidence.repetitions[0].batch = 32),
            Box::new(|evidence| evidence.repetitions[0].claim.operation = "append".into()),
            Box::new(|evidence| evidence.repetitions[1] = evidence.repetitions[0].clone()),
        ];
        for mutate in mutations {
            let mut evidence = smoke_evidence();
            mutate(&mut evidence);
            assert!(verify_evidence(&evidence).is_err());
        }
    }
}
