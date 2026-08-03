//! Release-evidence verification ledger (TP-001/TP-002/TP-003).
//!
//! A *verification ledger* is an append-only JSONL file under `target/fireweed-ledger/`. Each line is one
//! [`LedgerRow`] recording a measured release-evidence run: which suite/command produced it, the
//! backend profile + scale + seed + environment it ran under, the acceptance/invariant ids and TP-002
//! evidence ids it substantiates, the pass bar, the exit status, and the measured values. Evidence suites
//! append rows via [`append_row`]; the CI gate runs the `fireweed-verify-ledger` binary to strict-validate a
//! ledger and assert that required evidence ids (E0–E3) are present.
//!
//! This is the hexagonal-era rebuild of the ledger schema + verifier that lived in the removed
//! `fireweed-service` crate. The required fields match what `scripts/ci/release-gate.sh` validates.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub mod attestation;
pub mod e2_failover;
pub mod e3_contract;
pub mod evidence_io;
pub mod source_boundary;
pub mod transaction;

pub use evidence_io::{
    EvidenceIoError, EvidenceOperation, Fixture, Promoted, ReadableEvidence, RunOwned,
};
pub use source_boundary::{SourceBoundary, SourceBoundaryError, SourceInput, SourceInputKind};

/// Portable TP-002 E0/E1 evidence validation. Wall-clock rates and latency
/// percentiles are retained as capacity observations, but never decide this
/// contract.
pub mod single_deployment {
    use super::{LedgerRow, verify_ledger};
    use std::path::Path;

    fn true_value(row: &LedgerRow, key: &str) -> bool {
        row.measurements
            .values
            .get(key)
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    }

    fn u64_value(row: &LedgerRow, key: &str) -> Option<u64> {
        row.measurements
            .values
            .get(key)
            .and_then(serde_json::Value::as_u64)
    }

    fn positive_finite_value(row: &LedgerRow, key: &str) -> bool {
        row.measurements
            .values
            .get(key)
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|value| value.is_finite() && value > 0.0)
    }

    fn u64_array(row: &LedgerRow, key: &str) -> Option<Vec<u64>> {
        row.measurements
            .values
            .get(key)?
            .as_array()?
            .iter()
            .map(serde_json::Value::as_u64)
            .collect()
    }

    fn u64_map(row: &LedgerRow, key: &str) -> Option<std::collections::BTreeMap<String, u64>> {
        row.measurements
            .values
            .get(key)?
            .as_object()?
            .iter()
            .map(|(key, value)| Some((key.clone(), value.as_u64()?)))
            .collect()
    }

    fn measured_progress_errors(row: &LedgerRow) -> Vec<String> {
        let mut errors = Vec::new();
        let samples = u64_array(row, "progress_samples_finalized");
        match samples {
            Some(samples) => {
                if samples.len() < 3
                    || u64_value(row, "progress_sample_count") != Some(samples.len() as u64)
                    || samples.first() != Some(&0)
                    || samples.last() != Some(&10_000_000)
                    || !samples.windows(2).all(|window| window[1] >= window[0])
                    || !samples.windows(2).any(|window| window[1] > window[0])
                {
                    errors.push(
                        "finalized progress samples must be counted, start at zero, end at 10000000, and advance monotonically"
                            .into(),
                    );
                }
            }
            None => errors.push("measured finalized-count progress samples are required".into()),
        }
        let cursors = u64_array(row, "cursor_samples").unwrap_or_default();
        if cursors.len() < 3
            || !cursors.windows(2).all(|window| window[1] >= window[0])
            || !cursors.windows(2).any(|window| window[1] > window[0])
        {
            errors.push("command-position cursor samples must be monotonic and advance".into());
        }
        let bound = u64_value(row, "progress_bound_ms");
        let oldest = u64_array(row, "oldest_eligible_age_samples_ms").unwrap_or_default();
        let buckets = u64_map(row, "progress_latency_upper_buckets").unwrap_or_default();
        let bucket_count: u64 = buckets.values().sum();
        let over_60s = buckets.get("gt_60000").copied().unwrap_or(0);
        let bound_buckets = u64_map(row, "progress_bound_buckets").unwrap_or_default();
        let bound_bucket_count: u64 = bound_buckets.values().sum();
        let violations = bound_buckets
            .get("over_declared_bound")
            .copied()
            .unwrap_or(0);
        let lower = u64_value(row, "progress_latency_lower_max_ms");
        let upper = u64_value(row, "progress_latency_upper_max_ms");
        let valid_interval = matches!((lower, upper, bound), (Some(lower), Some(upper), Some(bound)) if lower <= upper && upper <= bound);
        if bound.is_none_or(|value| value == 0)
            || !true_value(row, "progress_bound_explicit")
            || u64_value(row, "persisted_progress_bound_ms") != bound
            || oldest.iter().any(|value| Some(*value) > bound)
            || oldest.is_empty()
            || u64_value(row, "discovery_query_count").is_none_or(|count| count == 0)
            || u64_value(row, "discovery_nonempty_count").is_none_or(|count| {
                count == 0 || count > u64_value(row, "discovery_query_count").unwrap_or(0)
            })
            || u64_value(row, "progress_identity_sample_count") != Some(10_000_000)
            || buckets.keys().map(String::as_str).collect::<Vec<_>>()
                != ["gt_60000", "le_1000", "le_10000", "le_60000"]
            || bucket_count != 10_000_000
            || bound_buckets.keys().map(String::as_str).collect::<Vec<_>>()
                != ["over_declared_bound", "within_declared_bound"]
            || bound_bucket_count != 10_000_000
            || !valid_interval
            || u64_value(row, "progress_latency_over_60000_ms_count") != Some(over_60s)
            || u64_value(row, "progress_bound_violations") != Some(violations)
            || violations != 0
            || !true_value(row, "fixed_latency_buckets_capacity_only")
            || row
                .measurements
                .values
                .get("progress_measurement")
                .and_then(serde_json::Value::as_str)
                != Some("per-item accepted and claimed timestamp intervals")
        {
            errors.push("the release workload must explicitly declare the persisted queue progress_bound_ms product-liveness contract, and all 10000000 accepted identities and discovery ages must satisfy it; host throughput, percentiles, and fixed timing buckets remain capacity observations and are not release gates".into());
        }
        errors
    }

    fn measured_resource_errors(row: &LedgerRow) -> Vec<String> {
        let mut errors = Vec::new();
        if u64_value(row, "resource_sample_count").is_none_or(|count| count < 3) {
            errors.push("at least three measured resource samples are required".into());
        }
        for (observed, limit) in [
            ("max_threads_observed", "thread_limit"),
            ("max_rss_bytes_observed", "rss_limit_bytes"),
        ] {
            if u64_value(row, observed)
                .is_none_or(|value| value == 0 || value > u64_value(row, limit).unwrap_or(0))
            {
                errors.push(format!(
                    "{observed} must be within the explicitly declared workload budget {limit}"
                ));
            }
        }
        for (peak, limit) in [
            ("shared_workers_peak", "shared_workers_limit"),
            ("connections_peak", "connections_limit"),
            ("pending_work_items_peak", "pending_work_items_limit"),
            ("memory_peak_bytes", "memory_limit_bytes"),
        ] {
            if u64_value(row, peak)
                .is_none_or(|value| value == 0 || value > u64_value(row, limit).unwrap_or(0))
            {
                errors.push(format!("{peak} must be measured and within {limit}"));
            }
        }
        for (alias_peak, alias_limit, source_peak, source_limit) in [
            (
                "connections_peak",
                "connections_limit",
                "max_connections_observed",
                "connection_limit",
            ),
            (
                "memory_peak_bytes",
                "memory_limit_bytes",
                "max_rss_bytes_observed",
                "rss_limit_bytes",
            ),
        ] {
            if u64_value(row, alias_peak) != u64_value(row, source_peak)
                || u64_value(row, alias_limit) != u64_value(row, source_limit)
            {
                errors.push(format!(
                    "{alias_peak}/{alias_limit} must be derived from {source_peak}/{source_limit}"
                ));
            }
        }
        if u64_value(row, "configured_concurrency") != Some(2)
            || u64_value(row, "workers_started") != Some(2)
            || u64_value(row, "workers_completed") != Some(2)
            || u64_value(row, "shared_workers_peak") != Some(2)
            || u64_value(row, "max_in_flight_operations_observed").is_none_or(|value| value < 2)
        {
            errors.push(
                "ordinary load must start, overlap, and complete two independent workers".into(),
            );
        }
        if u64_value(row, "max_in_flight_operations_observed")
            .is_none_or(|value| value > u64_value(row, "in_flight_operation_limit").unwrap_or(0))
            || u64_value(row, "shared_workers_limit").is_none_or(|value| value < 2)
        {
            errors.push(
                "natural operation overlap and workers must remain within explicit caps".into(),
            );
        }
        if u64_value(row, "max_connections_observed") != Some(2)
            || u64_value(row, "connection_limit").is_none_or(|limit| limit < 2)
        {
            errors.push(
                "two independent labeled Postgres production connections must be observed".into(),
            );
        }
        if row
            .measurements
            .values
            .get("resource_measurement_source")
            .and_then(serde_json::Value::as_str)
            != Some(
                "linux_procfs+declared_workload_caps+postgres_pg_stat_activity+natural_operation_counter",
            )
        {
            errors.push("resource measurement source is missing or unsupported".into());
        }
        for key in [
            "postgres_server_version",
            "postgres_instance_class",
            "postgres_iops_profile",
            "postgres_storage_class",
            "topology",
        ] {
            if row
                .measurements
                .values
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty)
            {
                errors.push(format!("{key} topology declaration is required"));
            }
        }
        for key in [
            "postgres_max_connections",
            "postgres_shared_buffers_bytes",
            "postgres_database_size_bytes",
            "host_cpu_count",
            "host_memory_bytes",
            "postgres_cpu_limit",
            "postgres_memory_limit_bytes",
            "postgres_pool_limit",
        ] {
            if u64_value(row, key).is_none_or(|value| value == 0) {
                errors.push(format!(
                    "{key} must be measured or declared as a positive value"
                ));
            }
        }
        for (key, expected) in [
            (
                "producer_completion_timing",
                "sum of successful push operation durations",
            ),
            (
                "claimant_completion_timing",
                "sum of successful claim and finalize operation durations",
            ),
        ] {
            if row
                .measurements
                .values
                .get(key)
                .and_then(serde_json::Value::as_str)
                != Some(expected)
            {
                errors.push(format!("{key} must identify its distinct operation timing"));
            }
        }
        let snapshots = row
            .measurements
            .values
            .get("lifecycle_snapshots")
            .and_then(serde_json::Value::as_array);
        let valid_snapshot = |value: &serde_json::Value| {
            let object = value.as_object()?;
            let pending = object.get("pending")?.as_u64()?;
            let leased = object.get("leased")?.as_u64()?;
            let complete = object.get("complete")?.as_u64()?;
            let failed = object.get("failed")?.as_u64()?;
            let resident_terminal = object.get("resident_terminal_count")?.as_u64()?;
            let cursor = object.get("cursor")?.as_u64()?;
            Some((pending, leased, complete, failed, resident_terminal, cursor))
        };
        let parsed = snapshots.and_then(|values| {
            values
                .iter()
                .map(valid_snapshot)
                .collect::<Option<Vec<_>>>()
        });
        if row
            .measurements
            .values
            .get("telemetry_surface")
            .and_then(serde_json::Value::as_str)
            != Some("Fireweed::metrics+current_position+discover_active_scopes")
            || parsed.as_ref().is_none_or(|values| values.len() < 3)
            || u64_value(row, "telemetry_sample_count")
                != parsed.as_ref().map(|values| values.len() as u64)
            || parsed
                .as_ref()
                .and_then(|values| values.last())
                .is_none_or(|last| *last != (0, 0, 10_000_000, 0, 10_000_000, last.5))
        {
            errors.push("real Fireweed lifecycle telemetry snapshots must be parseable, counted, and end at the exact 10M checkpoint".into());
        }
        if !true_value(row, "topology_declared") {
            errors.push(
                "release topology must be explicitly declared by the producer environment".into(),
            );
        }
        if row
            .measurements
            .values
            .get("topology")
            .and_then(serde_json::Value::as_str)
            != Some("single-process+single-postgres+fixed-2-member-affinity-pool")
        {
            errors.push(
                "topology must declare the fixed two-member queue-affinity production pool".into(),
            );
        }
        errors
    }

    fn nonportable(text: &str) -> bool {
        let text = text.to_ascii_lowercase();
        [
            "quiet host",
            "quiet-host",
            "idle host",
            "items/s >=",
            "throughput >=",
            "latency <=",
            "p95 <",
            "p99 <",
            "sub-second",
        ]
        .iter()
        .any(|needle| text.contains(needle))
    }

    pub fn validate_row(row: &LedgerRow, id: &str, revision: &str) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if !matches!(id, "E0" | "E1") {
            errors.push("id must be E0 or E1".into());
        }
        if row.suite != "performance_single_deployment_baseline_tests" {
            errors.push("suite must be performance_single_deployment_baseline_tests".into());
        }
        if row.backend_profile != "postgres_native" {
            errors.push("backend must be postgres_native".into());
        }
        if row.scale != "release" || row.evidence_tier != "release" {
            errors.push("row must be release tier and scale".into());
        }
        if row.measurements.tp002_evidence_ids.as_slice() != [id] {
            errors.push(format!("row must carry exactly {id}"));
        }
        if nonportable(&format!("{} {}", row.environment, row.pass_bar)) {
            errors.push("authority text contains a nonportable host-speed gate".into());
        }
        for key in [
            "bars_met",
            "portable_gate",
            "wall_clock_capacity_only",
            "exact_outcomes",
            "monotonic_progress",
            "bounded_resources",
        ] {
            if !true_value(row, key) {
                errors.push(format!("{key} must be true"));
            }
        }
        for key in ["quiet_host_required", "host_speed_gate"] {
            if row
                .measurements
                .values
                .get(key)
                .and_then(serde_json::Value::as_bool)
                != Some(false)
            {
                errors.push(format!("{key} must be false"));
            }
        }
        if row
            .measurements
            .values
            .get("source_revision")
            .and_then(serde_json::Value::as_str)
            != Some(revision)
        {
            errors.push("source_revision does not match expected revision".into());
        }
        if row
            .measurements
            .values
            .get("checkout_revision")
            .and_then(serde_json::Value::as_str)
            != Some(revision)
        {
            errors
                .push("checkout_revision does not match the exact verified source revision".into());
        }
        if !true_value(row, "checkout_clean")
            || !true_value(row, "source_root_explicit")
            || !true_value(row, "compile_source_root_bound")
            || row
                .measurements
                .values
                .get("checkout_root")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty)
            || row
                .measurements
                .values
                .get("compile_source_root")
                .and_then(serde_json::Value::as_str)
                != row
                    .measurements
                    .values
                    .get("checkout_root")
                    .and_then(serde_json::Value::as_str)
        {
            errors.push("release evidence requires an exact clean producing checkout root bound to the compiled source root".into());
        }
        for key in [
            "producer_ingest_completion_per_s",
            "claimant_finalize_completion_per_s",
            "producer_completion_ms",
            "claimant_completion_ms",
        ] {
            if !positive_finite_value(row, key) {
                errors.push(format!(
                    "{key} must be a finite positive capacity observation"
                ));
            }
        }
        if u64_value(row, "resident_set_items") != Some(10_000_000)
            || u64_value(row, "retained_terminal_items") != Some(10_000_000)
        {
            errors.push(
                "the measured checkpoint must retain exactly 10000000 terminal resident items"
                    .into(),
            );
        }
        if u64_value(row, "lost_items") != Some(0) || u64_value(row, "duplicate_claims") != Some(0)
        {
            errors.push("lost_items and duplicate_claims must be zero".into());
        }
        let counter_min = u64_value(row, "identity_counter_min");
        let counter_max = u64_value(row, "identity_counter_max");
        if !true_value(row, "identity_bijection")
            || u64_value(row, "identity_epoch_node_prefix").is_none_or(|value| value == 0)
            || counter_min.is_none()
            || counter_max.is_none()
            || counter_max
                .zip(counter_min)
                .is_none_or(|(max, min)| max < min || max - min + 1 != 10_000_000)
        {
            errors.push("full ItemId epoch/node prefix and contiguous counter bijection must reconcile all 10M identities".into());
        }
        errors.extend(measured_progress_errors(row));
        errors.extend(measured_resource_errors(row));
        if u64_value(row, "checkpoint_pending") != Some(0)
            || u64_value(row, "checkpoint_leased") != Some(0)
            || u64_value(row, "checkpoint_complete") != Some(10_000_000)
            || u64_value(row, "checkpoint_failed") != Some(0)
        {
            errors.push("checkpoint lifecycle reconciliation must be pending=0 leased=0 complete=10000000 failed=0".into());
        }
        let payloads = u64_map(row, "payload_size_counts").unwrap_or_default();
        let groups = u64_array(row, "group_item_counts").unwrap_or_default();
        let priorities = u64_map(row, "priority_class_counts").unwrap_or_default();
        let mix = u64_map(row, "workload_operation_mix").unwrap_or_default();
        if payloads.keys().map(String::as_str).collect::<Vec<_>>() != ["1024", "2048", "512"]
            || payloads.values().sum::<u64>() != 10_000_000
            || payloads.values().any(|count| *count == 0)
            || groups.len() != 64
            || groups.iter().sum::<u64>() != 10_000_000
            || groups.contains(&0)
            || priorities.keys().map(String::as_str).collect::<Vec<_>>()
                != ["high", "regular", "sentinel"]
            || priorities.values().sum::<u64>() != 10_000_000
            || priorities.values().any(|count| *count == 0)
            || mix.get("push_batches").copied().unwrap_or(0) == 0
            || mix.get("claim_batches").copied().unwrap_or(0) == 0
            || mix.get("claim_batches") != mix.get("finalize_batches")
        {
            errors.push("measured payload histogram, 64-group counts, priority counts, and operation mix must reconcile to 10M".into());
        }
        if id == "E0" {
            if !true_value(row, "one_instance_production_wrapper")
                || u64_value(row, "production_pool_size") != Some(2)
                || u64_value(row, "production_pool_connections_observed") != Some(2)
                || u64_value(row, "hot_queue_pool_partition")
                    == u64_value(row, "canary_queue_pool_partition")
                || !true_value(row, "canary_observed_hot_pg_sleep")
                || !true_value(row, "canary_exact_outcomes")
                || !true_value(row, "canary_completed_before_hot")
                || !true_value(row, "canary_causal_progress")
            {
                errors.push("E0 must come from one PostgresWholeOperationAdapter with exactly two production connections and an affinity-routed canary that causally releases the sleeping hot member before hot completion".into());
            }
            if u64_value(row, "accepted_items") != Some(10_000_000)
                || u64_value(row, "claimed_items") != Some(10_000_000)
                || u64_value(row, "finalized_items") != Some(10_000_000)
            {
                errors
                    .push("E0 exact accepted/claimed/finalized counts must equal 10000000".into());
            }
        } else {
            for operation in ["push", "update_window", "claim", "finalize"] {
                for batch_size in [1, 100, 1_000] {
                    for percentile in ["p50", "p95", "p99"] {
                        let key = format!("{operation}_b{batch_size}_{percentile}_ms");
                        if !positive_finite_value(row, &key) {
                            errors.push(format!(
                                "{key} must be a finite positive capacity observation"
                            ));
                        }
                    }
                }
            }
            let configured_max = u64_value(row, "configured_max_batch_size");
            if configured_max != Some(1_000)
                || u64_value(row, "persisted_max_push_batch_size") != configured_max
                || u64_value(row, "persisted_max_claim_batch_size") != configured_max
                || !true_value(row, "oversize_push_rejected")
                || !true_value(row, "oversize_claim_rejected")
            {
                errors.push("E1 must prove the persisted 1000-item facade limit and production-path rejection at 1001".into());
            }
            for key in [
                "push_batch_sizes",
                "update_window_sizes",
                "claim_batch_sizes",
                "finalize_batch_sizes",
            ] {
                if u64_array(row, key).as_deref() != Some(&[1, 100, 1_000]) {
                    errors.push(format!("{key} must record actual [1,100,1000] operations"));
                }
            }
            let accepted = u64_value(row, "probe_accepted_items");
            let probe_mix = u64_map(row, "probe_operation_mix").unwrap_or_default();
            if accepted.is_none()
                || accepted != u64_value(row, "probe_claimed_items")
                || accepted != u64_value(row, "probe_finalized_items")
                || u64_value(row, "total_accepted_items")
                    != accepted.map(|count| 10_000_000 + count)
                || u64_value(row, "total_claimed_items") != accepted.map(|count| 10_000_000 + count)
                || u64_value(row, "total_finalized_items")
                    != accepted.map(|count| 10_000_000 + count)
                || u64_value(row, "probe_unique_accepted_ids") != accepted
                || u64_value(row, "probe_unique_claimed_ids") != accepted
                || u64_value(row, "probe_unique_finalized_ids") != accepted
                || !true_value(row, "probe_identity_exact")
                || u64_value(row, "post_probe_pending") != Some(0)
                || u64_value(row, "post_probe_leased") != Some(0)
                || u64_value(row, "post_probe_complete") != accepted.map(|count| 10_000_000 + count)
                || u64_value(row, "post_probe_failed") != Some(0)
                || u64_value(row, "post_probe_resident_terminal_count")
                    != accepted.map(|count| 10_000_000 + count)
                || probe_mix.get("push_batches").copied().unwrap_or(0) == 0
                || probe_mix.get("push_items").copied() != accepted
                || probe_mix.get("claim_items").copied() != accepted
                || probe_mix.get("finalize_items").copied() != accepted
                || probe_mix.get("claim_batches") != probe_mix.get("push_batches")
                || probe_mix.get("finalize_batches") != probe_mix.get("push_batches")
                || probe_mix
                    .get("update_item_calls")
                    .copied()
                    .is_none_or(|count| count == 0 || Some(count) > accepted)
                || !true_value(row, "post10m_affinity_serialization_probe")
                || !true_value(row, "post10m_caller_interval_overlap_observed")
                || u64_value(row, "post10m_caller_in_flight_observed").is_none_or(|value| value < 2)
                || u64_value(row, "post10m_active_pending_before") != Some(1_000)
            {
                errors.push(
                    "E1 probe accepted/claimed/finalized counts must reconcile exactly".into(),
                );
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn verify_file(path: &Path, id: &str, revision: &str) -> Result<(), Vec<String>> {
        verify_ledger(path, true).map_err(|es| es.into_iter().map(|e| e.0).collect::<Vec<_>>())?;
        let body = std::fs::read_to_string(path).map_err(|e| vec![e.to_string()])?;
        let mut lines = body.lines().filter(|line| !line.trim().is_empty());
        let line = lines.next().ok_or_else(|| vec!["ledger is empty".into()])?;
        if lines.next().is_some() {
            return Err(vec!["ledger must contain exactly one row".into()]);
        }
        let row = serde_json::from_str(line).map_err(|e| vec![e.to_string()])?;
        validate_row(&row, id, revision)
    }
}

/// One verification-ledger row: a single measured release-evidence run.
///
/// Every field is required (a row that fails to deserialize is rejected by the verifier). `measurements`
/// carries the TP-002 evidence ids plus the measured values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerRow {
    /// The named test suite that produced this row (e.g. `performance_cross_queue_scale_out_tests`).
    pub suite: String,
    /// The exact command that produced it (for reproduction).
    pub command: String,
    /// Backend profile under test (`postgres_native` | `object_log_sqlite_projection` | `memory` | ...).
    pub backend_profile: String,
    /// Scale shape (`smoke` | `release` | a specific `S=…` descriptor).
    pub scale: String,
    /// Deterministic seed for the run (`0` = no seed / wall-clock-timed run with no seeded randomness).
    pub seed: u64,
    /// Where it ran (host class / CI lane / `in-process`).
    pub environment: String,
    /// Process exit status of the producing command. A non-zero status is NOT evidence (the run failed).
    pub exit_status: i32,
    /// Acceptance-criterion ids this row substantiates (e.g. `AC-E2E-1`). May be empty for pure scale rows.
    #[serde(default)]
    pub ac_ids: Vec<String>,
    /// Invariant ids held during the run (e.g. `INV-1`). May be empty.
    #[serde(default)]
    pub inv_ids: Vec<String>,
    /// The pass bar this row was judged against (human-readable).
    pub pass_bar: String,
    /// Evidence tier: `release` (counts toward the headline E0–E3 requirement) or `smoke` (an in-process or
    /// reduced-scale run — recorded and strict-validated for visibility, but NOT accepted as headline
    /// evidence by the gate). The serde default preserves legacy deserialization compatibility, but an
    /// absent tier is non-authoritative and strict verification rejects it. Headline evidence requires an
    /// explicit, exact `release` tier.
    #[serde(default = "default_tier")]
    pub evidence_tier: String,
    /// Measured values + the TP-002 evidence ids substantiated.
    pub measurements: Measurements,
}

fn default_tier() -> String {
    "release".to_string()
}

/// Measured values for a row. [`tp002_evidence_ids`](Self::tp002_evidence_ids) names the E0–E3 records this
/// row substantiates; arbitrary additional measured key/values are kept in [`values`](Self::values).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Measurements {
    /// TP-002 evidence ids this row substantiates (`E0` | `E1` | `E2` | `E3`).
    #[serde(default)]
    pub tp002_evidence_ids: Vec<String>,
    /// Any additional measured values (throughput, latency percentiles, recovery time, …).
    #[serde(flatten, default)]
    pub values: BTreeMap<String, serde_json::Value>,
}

impl LedgerRow {
    /// Serialize this row to a single JSONL line (no trailing newline).
    pub fn to_jsonl(&self) -> String {
        serde_json::to_string(self).expect("LedgerRow serializes")
    }
}

/// The ledger file an evidence suite writes its row to: `<dir>/<suite>.jsonl`, where `<dir>` is
/// `$FIREWEED_LEDGER_DIR` if set, else a process-unique directory below the system temporary root. The
/// returned [`RunOwned`] rejects repository-owned and tracked-evidence targets before any write occurs.
///
/// NOTE: this ledger-directory read is the ONE intentional library `std::env` access in the workspace. It
/// is CI / test-evidence tooling (where validation suites drop their JSONL ledger rows), NOT server runtime
/// configuration — so it is exempt from the "no env reads in library runtime code" rule. The runtime
/// `Config` populator (`fireweed_server::Config::from_env`) is the only env→config path for the server itself.
pub fn ledger_path(manifest_dir: &str, suite: &str) -> Result<RunOwned, EvidenceIoError> {
    static NEXT_LEDGER: AtomicU64 = AtomicU64::new(0);

    let repository_root = Path::new(manifest_dir).join("../..").canonicalize()?;
    let dir = match std::env::var("FIREWEED_LEDGER_DIR") {
        Ok(value) if !value.trim().is_empty() => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                path
            } else {
                repository_root.join(path)
            }
        }
        _ => {
            let serial = NEXT_LEDGER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("fireweed-ledger-{}-{serial}", std::process::id()));
            fs::create_dir_all(&path)?;
            path
        }
    };
    RunOwned::new(&repository_root, &dir, format!("{suite}.jsonl"))
}

/// Append one row to the ledger at `path`, creating the file (and parent dirs) if needed. The whole line —
/// JSON body AND trailing newline — is written in a SINGLE `write_all`, so under the OS append flag
/// concurrent appenders stay line-atomic for lines below the platform atomic-append size (PIPE_BUF). (A
/// `writeln!` would emit the body and `"\n"` as two separate writes, which O_APPEND could interleave.)
pub fn append_row(path: &RunOwned, row: &LedgerRow) -> io::Result<()> {
    let path = path
        .authorize(EvidenceOperation::Write)
        .map_err(io::Error::other)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(format!("{}\n", row.to_jsonl()).as_bytes())
}

/// A validation finding (a reason a ledger row or the ledger as a whole is not acceptable evidence).
#[derive(Debug, Clone, PartialEq)]
pub struct LedgerError(pub String);

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Outcome of validating a ledger: the rows seen, the union of evidence ids RELEASE-tier rows substantiate
/// (`evidence_ids`, the only ones the headline requirement counts), and — for visibility — the evidence ids
/// only seen on `smoke`-tier rows.
#[derive(Debug, Clone, Default)]
pub struct LedgerSummary {
    pub rows: usize,
    pub evidence_ids: std::collections::BTreeSet<String>,
    pub smoke_evidence_ids: std::collections::BTreeSet<String>,
}

/// A governed TP-002 manifest names the exact ledger file authoritative for each evidence ID.
/// Broad directory scans are intentionally not supported by this format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub schema_version: u32,
    pub authorities: Vec<ReleaseAuthority>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseAuthority {
    pub evidence_id: String,
    pub path: String,
}

/// Validate a ledger file. In `strict` mode each row must be well-formed AND acceptable evidence (exit 0,
/// non-empty identifying fields, and traceable to at least one acceptance or evidence id). Returns the
/// [`LedgerSummary`] on success, or every [`LedgerError`] found.
pub fn verify_ledger(path: &Path, strict: bool) -> Result<LedgerSummary, Vec<LedgerError>> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return Err(vec![LedgerError(format!(
                "cannot open ledger {path:?}: {e}"
            ))]);
        }
    };
    let mut errors = Vec::new();
    let mut summary = LedgerSummary::default();
    for (i, line) in io::BufReader::new(file).lines().enumerate() {
        let lineno = i + 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: read error: {e}")));
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let raw: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: malformed row: {e}")));
                continue;
            }
        };
        let explicit_tier = raw
            .get("evidence_tier")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let row: LedgerRow = match serde_json::from_value(raw) {
            Ok(r) => r,
            Err(e) => {
                errors.push(LedgerError(format!("line {lineno}: malformed row: {e}")));
                continue;
            }
        };
        summary.rows += 1;
        // Only RELEASE-tier rows count toward the headline E0–E3 requirement; smoke-tier rows are recorded
        // separately so an in-process/reduced-scale run can never satisfy a release-evidence gate.
        let ids = row.measurements.tp002_evidence_ids.iter().cloned();
        match explicit_tier.as_deref() {
            Some("release") => summary.evidence_ids.extend(ids),
            Some("smoke") => summary.smoke_evidence_ids.extend(ids),
            _ => {}
        }
        if strict {
            if !matches!(explicit_tier.as_deref(), Some("release" | "smoke")) {
                errors.push(LedgerError(format!(
                    "line {lineno} ({}): evidence_tier must be explicitly release or smoke",
                    row.suite
                )));
            }
            for e in strict_row_errors(&row) {
                errors.push(LedgerError(format!("line {lineno} ({}): {e}", row.suite)));
            }
        }
    }
    if strict && summary.rows == 0 {
        errors.push(LedgerError("ledger is empty".into()));
    }
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(errors)
    }
}

/// Strict-mode acceptability checks for a single row.
fn strict_row_errors(row: &LedgerRow) -> Vec<String> {
    let mut e = Vec::new();
    if !matches!(row.evidence_tier.as_str(), "release" | "smoke") {
        e.push("evidence_tier must be release or smoke".into());
    }
    if row.exit_status != 0 {
        e.push(format!(
            "exit_status {} != 0 (a failed run is not evidence)",
            row.exit_status
        ));
    }
    if row.suite.trim().is_empty() {
        e.push("empty suite".into());
    }
    if row.command.trim().is_empty() {
        e.push("empty command".into());
    }
    if row.backend_profile.trim().is_empty() {
        e.push("empty backend_profile".into());
    }
    if row.scale.trim().is_empty() {
        e.push("empty scale".into());
    }
    if row.environment.trim().is_empty() {
        e.push("empty environment".into());
    }
    if row.pass_bar.trim().is_empty() {
        e.push("empty pass_bar".into());
    }
    if row.ac_ids.is_empty() && row.measurements.tp002_evidence_ids.is_empty() {
        e.push("row cites no ac_ids and no tp002_evidence_ids (untraceable)".into());
    }
    e
}

/// Validate EVERY `*.jsonl` ledger in `dir`, merging the per-file summaries (rows, release-tier
/// `evidence_ids`, and `smoke_evidence_ids`). The gate emits one file per suite into a clean dir, so this
/// aggregates the whole run. Returns the merged [`LedgerSummary`] or every [`LedgerError`] across all files.
pub fn verify_ledger_dir(dir: &Path, strict: bool) -> Result<LedgerSummary, Vec<LedgerError>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            return Err(vec![LedgerError(format!(
                "cannot read ledger dir {dir:?}: {e}"
            ))]);
        }
    };
    let mut paths: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    paths.sort();
    let mut merged = LedgerSummary::default();
    let mut errors = Vec::new();
    for p in &paths {
        match verify_ledger(p, strict) {
            Ok(s) => {
                merged.rows += s.rows;
                merged.evidence_ids.extend(s.evidence_ids);
                merged.smoke_evidence_ids.extend(s.smoke_evidence_ids);
            }
            Err(es) => errors.extend(
                es.into_iter()
                    .map(|e| LedgerError(format!("{}: {}", p.display(), e.0))),
            ),
        }
    }
    if strict && paths.is_empty() {
        errors.push(LedgerError(format!("no *.jsonl ledger files in {dir:?}")));
    }
    if errors.is_empty() {
        Ok(merged)
    } else {
        Err(errors)
    }
}

/// Verify the exact TP-002 authority files listed by a governed release manifest.
///
/// Each authority must be one strict-valid ledger containing exactly one row. The row must claim only
/// the listed E-ID, be release-tier at release scale, report `bars_met: true`, and use the profile governed
/// for that E-ID. Manifest paths are relative to the manifest and cannot escape its directory.
pub fn verify_release_manifest(path: &Path) -> Result<LedgerSummary, Vec<LedgerError>> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) => {
            return Err(vec![LedgerError(format!(
                "cannot read release manifest {}: {error}",
                path.display()
            ))]);
        }
    };
    let manifest: ReleaseManifest = match serde_json::from_slice(&contents) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Err(vec![LedgerError(format!(
                "malformed release manifest {}: {error}",
                path.display()
            ))]);
        }
    };

    let mut errors = Vec::new();
    if manifest.schema_version != 1 {
        errors.push(LedgerError(format!(
            "unsupported release manifest schema_version {}; expected 1",
            manifest.schema_version
        )));
    }
    if manifest.authorities.is_empty() {
        errors.push(LedgerError(
            "release manifest has no authority entries".into(),
        ));
    }

    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let mut evidence_ids = std::collections::BTreeSet::new();
    let mut authority_paths = std::collections::BTreeSet::new();
    let mut rows = 0;
    for authority in manifest.authorities {
        let id = authority.evidence_id.as_str();
        if !matches!(id, "E0" | "E1" | "E2" | "E3") {
            errors.push(LedgerError(format!(
                "unknown TP-002 evidence id {:?}",
                authority.evidence_id
            )));
            continue;
        }
        if !evidence_ids.insert(authority.evidence_id.clone()) {
            errors.push(LedgerError(format!(
                "duplicate authority for evidence id {id}"
            )));
            continue;
        }
        if !safe_manifest_path(&authority.path) {
            errors.push(LedgerError(format!(
                "authority path {:?} is not a safe manifest-relative path",
                authority.path
            )));
            continue;
        }
        if !authority_paths.insert(authority.path.clone()) {
            errors.push(LedgerError(format!(
                "authority file {:?} is listed more than once",
                authority.path
            )));
            continue;
        }

        let ledger_path = base.join(&authority.path);
        match verify_ledger(&ledger_path, true) {
            Ok(summary) if summary.rows != 1 => {
                errors.push(LedgerError(format!(
                    "authority {id} file {:?} contains {} rows; expected exactly one",
                    authority.path, summary.rows
                )));
                continue;
            }
            Ok(_) => {}
            Err(file_errors) => {
                errors.extend(file_errors.into_iter().map(|error| {
                    LedgerError(format!(
                        "authority {id} file {:?}: {}",
                        authority.path, error.0
                    ))
                }));
                continue;
            }
        }

        let (row, raw_row) = match read_single_ledger_row(&ledger_path) {
            Ok(row) => row,
            Err(error) => {
                errors.push(LedgerError(format!(
                    "authority {id} file {:?}: {error}",
                    authority.path
                )));
                continue;
            }
        };
        rows += 1;
        for error in release_authority_errors(id, &row, &raw_row) {
            errors.push(LedgerError(format!(
                "authority {id} file {:?}: {error}",
                authority.path
            )));
        }
    }
    for required in ["E0", "E1", "E2", "E3"] {
        if !evidence_ids.contains(required) {
            errors.push(LedgerError(format!(
                "release manifest is missing authority for {required}"
            )));
        }
    }

    if errors.is_empty() {
        Ok(LedgerSummary {
            rows,
            evidence_ids,
            smoke_evidence_ids: Default::default(),
        })
    } else {
        Err(errors)
    }
}

fn safe_manifest_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn read_single_ledger_row(path: &Path) -> Result<(LedgerRow, serde_json::Value), String> {
    let contents = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut rows = contents.lines().filter(|line| !line.trim().is_empty());
    let line = rows
        .next()
        .ok_or_else(|| "authority ledger is empty".to_string())?;
    let raw: serde_json::Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let row = serde_json::from_value(raw.clone()).map_err(|error| error.to_string())?;
    Ok((row, raw))
}

fn release_authority_errors(id: &str, row: &LedgerRow, raw_row: &serde_json::Value) -> Vec<String> {
    let mut errors = Vec::new();
    let explicit_tier = raw_row
        .get("evidence_tier")
        .and_then(serde_json::Value::as_str);
    if explicit_tier != Some("release") {
        errors.push(format!(
            "evidence_tier must be explicitly and exactly \"release\", got {explicit_tier:?}"
        ));
    }
    if row.scale != "release" {
        errors.push(format!(
            "scale must be exactly \"release\", got {:?}",
            row.scale
        ));
    }
    if row.measurements.tp002_evidence_ids.as_slice() != [id] {
        errors.push(format!(
            "row evidence ids {:?} do not exactly match listed authority {id}",
            row.measurements.tp002_evidence_ids
        ));
    }
    match row.measurements.values.get("bars_met") {
        Some(serde_json::Value::Bool(true)) => {}
        Some(value) => errors.push(format!("bars_met must be boolean true, got {value}")),
        None => errors.push("bars_met is required and must be boolean true".into()),
    }
    if matches!(id, "E0" | "E1") {
        let text = format!("{} {}", row.environment, row.pass_bar).to_ascii_lowercase();
        let values = &row.measurements.values;
        let portable = values
            .get("portable_gate")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            && values
                .get("wall_clock_capacity_only")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            && values
                .get("quiet_host_required")
                .and_then(serde_json::Value::as_bool)
                == Some(false)
            && values
                .get("host_speed_gate")
                .and_then(serde_json::Value::as_bool)
                == Some(false);
        let nonportable_text = [
            "quiet host",
            "quiet-host",
            "idle host",
            "items/s >=",
            "throughput >=",
            "p95 <",
            "p99 <",
            "sub-second",
        ]
        .iter()
        .any(|needle| text.contains(needle));
        if !portable || nonportable_text {
            errors.push(format!(
                "{id} authority does not satisfy the portable semantic contract"
            ));
        }
    }
    let profile_allowed = match id {
        "E0" | "E1" => row.backend_profile == "postgres_native",
        "E2" => row.backend_profile == e2::RELEASE_BACKEND_PROFILE,
        "E3" => matches!(
            row.backend_profile.as_str(),
            "object_log_inmemory_projection" | "object_log_sqlite_projection"
        ),
        _ => false,
    };
    if !profile_allowed {
        errors.push(format!(
            "backend_profile {:?} is not governed for {id}; required E2 profile set is {:?}",
            row.backend_profile,
            [e2::RELEASE_BACKEND_PROFILE]
        ));
    }
    errors
}

/// Assert every id in `required` (e.g. `["E0","E1","E2","E3"]`) appears in some RELEASE-tier row's
/// `measurements.tp002_evidence_ids`. Returns the missing ids (empty = satisfied).
pub fn missing_evidence(summary: &LedgerSummary, required: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|id| !summary.evidence_ids.contains(*id))
        .cloned()
        .collect()
}

/// Like [`missing_evidence`] but against the SMOKE-tier evidence ids — for the gate's in-process smoke lane
/// (which records evidence but cannot satisfy the release headline).
pub fn missing_smoke_evidence(summary: &LedgerSummary, required: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|id| !summary.smoke_evidence_ids.contains(*id))
        .cloned()
        .collect()
}

/// TP-002 **E2** (cross-queue scale-out / ADR-008) release-bar judgment + ledger-row construction.
///
/// This is the SHARED, PURE judgment behind the TP-002 E2 verification-ledger row. The in-cluster load
/// generator (`fireweed-loadgen emit-row`) folds three per-owner-count measured scale points (owners 2/4/8)
/// into one row; this module decides whether that sweep cleared the four release bars and, if so, whether
/// the row is `release`-tier (counts toward the headline E0–E3 requirement) or `smoke`-tier. It is a pure
/// function of the measured inputs so the judgment is unit-testable from `fireweed-bench` WITHOUT provisioning
/// a live cluster.
///
/// The portable E2 release bars are canonical topology, complete positive measurements at every scale
/// point, and exact one-owner-per-queue isolation. Aggregate monotonicity, the 8/2 multiple, and absolute
/// per-queue rates are retained as declared-topology capacity observations; they are not release gates.
/// No queue may be served by more than one owner (live-proven: at 8 owners, every queue is unknown on every
///    OTHER node, so the cross-node "no such queue" confirmation count equals the expected
///    `owners * queues_per_owner * (owners - 1)`).
pub mod e2 {
    use super::{LedgerRow, Measurements};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// The resolved release-authority backend profile for TP-002 E2.
    ///
    /// `object_log_inmemory_projection` remains a comparator profile in the plan,
    /// but it is not a release authority for the headline E2 matrix.
    pub const RELEASE_BACKEND_PROFILE: &str = "object_log_sqlite_projection";
    pub const CANONICAL_SWEEPS: [u64; 3] = [1, 2, 3];
    pub const CANONICAL_ITEMS_PER_QUEUE: u64 = 12_000;
    pub const CANONICAL_CONNS_PER_QUEUE: u64 = 8;
    pub const CANONICAL_PIPE_SIZE: u64 = 1_000;
    pub const CANONICAL_BATCH_SIZE: u64 = 1_000;
    pub const CANONICAL_QUEUES_PER_OWNER: u64 = 1;
    pub const CANONICAL_SEGMENT_MAX_LATENCY_MS: u64 = 1;
    pub const CANONICAL_SEGMENT_TARGET_BYTES: u64 = 262_144;
    pub const CANONICAL_WORKER_THREADS_PER_NODE: u64 = 2;
    pub const CANONICAL_SERVER_CPU_LIMIT: &str = "1300m";
    pub const CANONICAL_SERVER_CPU_REQUEST: &str = "1000m";
    pub const CANONICAL_LOADGEN_CPU_LIMIT: &str = "2000m";

    /// Product capacity target retained in evidence for comparison only; never a host-independent gate.
    pub const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;
    /// Historical capacity target retained in evidence for comparison only; never a release gate.
    pub const SCALE_MULTIPLE_BAR: f64 = 3.5;
    /// The canonical owner counts an E2 sweep must cover (the bars compare 8 vs 2 and require 2→4→8 monotonic).
    pub const CANONICAL_OWNER_COUNTS: [usize; 3] = [2, 4, 8];

    /// One MEASURED scale point: the result of driving the segmented `object_log_sqlite_projection` workload
    /// at ONE owner count. Mirrors the load generator's per-run `RunResult` wire type (identical field names
    /// + serde shape) so the generator can use this directly as its `run`→`emit-row` wire type.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct E2ScalePoint {
        /// Owner-node count for this scale point.
        pub owners: usize,
        /// Aggregate ingest throughput (items/s) across all queues at this owner count.
        pub ingest_aggregate: f64,
        /// Worst (minimum) single-queue ingest throughput (items/s) at this owner count.
        pub ingest_min_per_queue: f64,
        /// Aggregate claim+finalize (drain) throughput (items/s) at this owner count.
        pub drain_aggregate: f64,
        /// Worst (minimum) single-queue claim+finalize throughput (items/s) at this owner count.
        pub drain_min_per_queue: f64,
        /// Cross-node "no such queue" confirmations observed (every queue rejected by every non-owner node).
        pub one_owner_confirmations: usize,
        /// Queues owned per node (disjoint across nodes).
        pub queues_per_owner: usize,
        /// Items driven per queue.
        pub items_per_queue: u64,
        /// Concurrent connections per queue.
        pub conns_per_queue: usize,
    }

    /// Per-node tuning recorded into the evidence row (passed by the orchestrator). Mirrors the load
    /// generator's `TuningMeta` wire type (identical field names + serde shape).
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct E2Tuning {
        pub source_revision: String,
        pub segment_max_latency_ms: u64,
        pub segment_target_bytes: usize,
        pub worker_threads_per_node: usize,
        pub server_cpu_limit: String,
        pub server_cpu_request: String,
        pub loadgen_cpu_limit: String,
        pub cores: usize,
        pub kind_node_image: String,
        pub pipe_size: usize,
        pub batch_size: usize,
        pub sweep: u64,
    }

    /// The judged verdict: each bar's pass/fail, the measured quantities the bars were judged from, and the
    /// AND of all four ([`bars_met`](Self::bars_met)) — the ONLY thing that promotes the row to `release`.
    #[derive(Debug, Clone, PartialEq)]
    pub struct E2Verdict {
        /// Whether the canonical owner counts (2/4/8) are all present — bars cannot pass without them.
        pub canonical_owners_present: bool,
        /// Capacity observation: whether ingest aggregate was non-decreasing 2 → 4 → 8.
        pub nondecreasing: bool,
        /// Bar (2): the measured 8-owner / 2-owner ingest aggregate ratio.
        pub ratio_8_2: f64,
        /// Portable measurement check: the 8/2 ratio is finite and positive (the 3.5x target is reporting only).
        pub scale_pass: bool,
        /// Bar (3): worst per-queue ingest throughput across all scale points.
        pub worst_ingest_per_queue: f64,
        /// Bar (3): worst per-queue claim+finalize throughput across all scale points.
        pub worst_drain_per_queue: f64,
        /// Portable progress check: every measured per-queue ingest/drain rate is finite and positive.
        pub floor_pass: bool,
        /// Bar (4): cross-node confirmations measured at 8 owners.
        pub one_owner_confirmations: usize,
        /// Bar (4): the confirmation count one-owner-per-queue requires at 8 owners.
        pub expected_confirmations: usize,
        /// Bar (4): every queue is served by exactly one owner (confirmations == expected, and queues exist).
        pub disjoint_pass: bool,
        /// The AND of all four bars. `true` ⇒ the row is release-tier; `false` ⇒ smoke-tier.
        pub bars_met: bool,
    }

    /// The cross-node "no such queue" confirmations one-owner-per-queue MUST produce at `owners` nodes each
    /// owning `queues_per_owner` queues: every one of the `owners * queues_per_owner` queues is probed on the
    /// `owners - 1` OTHER nodes and must be unknown on each. Fewer than this ⇒ some queue answered on more
    /// than one node ⇒ bar (4) fails.
    pub fn expected_one_owner_confirmations(owners: usize, queues_per_owner: usize) -> usize {
        owners * queues_per_owner * owners.saturating_sub(1)
    }

    /// Validate the cross-owner E2 authority independently of density and
    /// failover/routing authorities.
    pub fn validate_release_row(row: &LedgerRow, revision: &str) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        let values = &row.measurements.values;
        let bool_value = |key: &str| values.get(key).and_then(serde_json::Value::as_bool);
        let u64_value = |key: &str| values.get(key).and_then(serde_json::Value::as_u64);
        let positive = |key: &str| {
            values
                .get(key)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|v| v.is_finite() && v > 0.0)
        };
        if row.suite != "performance_multi_node_object_log_e2_kind" {
            errors.push("unexpected E2 scale suite".into());
        }
        if row.backend_profile != RELEASE_BACKEND_PROFILE {
            errors.push("unexpected E2 release backend".into());
        }
        if row.scale != "release" || row.evidence_tier != "release" {
            errors.push("E2 scale row must be release tier and scale".into());
        }
        if row.measurements.tp002_evidence_ids.as_slice() != ["E2"] {
            errors.push("E2 scale row must carry exactly E2".into());
        }
        if row.seed != 0 {
            errors.push("E2 scale row seed must be the canonical value 0".into());
        }
        for key in ["bars_met", "portable_gate", "wall_clock_capacity_only"] {
            if bool_value(key) != Some(true) {
                errors.push(format!("{key} must be true"));
            }
        }
        for key in ["quiet_host_required", "host_speed_gate"] {
            if bool_value(key) != Some(false) {
                errors.push(format!("{key} must be false"));
            }
        }
        if values
            .get("source_revision")
            .and_then(serde_json::Value::as_str)
            != Some(revision)
        {
            errors.push("source_revision does not match expected revision".into());
        }
        for key in [
            "owners_2_ingest_aggregate_per_s",
            "owners_4_ingest_aggregate_per_s",
            "owners_8_ingest_aggregate_per_s",
            "owners_2_claim_finalize_aggregate_per_s",
            "owners_4_claim_finalize_aggregate_per_s",
            "owners_8_claim_finalize_aggregate_per_s",
            "worst_ingest_per_queue_per_s",
            "worst_claim_finalize_per_queue_per_s",
        ] {
            if !positive(key) {
                errors.push(format!("{key} must be finite and positive"));
            }
        }
        let queues = u64_value("queues_per_owner").unwrap_or(0);
        let expected = expected_one_owner_confirmations(8, queues as usize) as u64;
        if queues == 0 || u64_value("one_owner_per_queue_confirmations") != Some(expected) {
            errors.push("one-owner-per-queue confirmation count is not exact".into());
        }
        for (key, expected) in [
            ("queues_per_owner", CANONICAL_QUEUES_PER_OWNER),
            ("items_per_queue", CANONICAL_ITEMS_PER_QUEUE),
            ("conns_per_queue", CANONICAL_CONNS_PER_QUEUE),
            ("pipe_size", CANONICAL_PIPE_SIZE),
            ("batch_size", CANONICAL_BATCH_SIZE),
            ("segment_max_latency_ms", CANONICAL_SEGMENT_MAX_LATENCY_MS),
            ("segment_target_bytes", CANONICAL_SEGMENT_TARGET_BYTES),
            ("worker_threads_per_node", CANONICAL_WORKER_THREADS_PER_NODE),
        ] {
            if u64_value(key) != Some(expected) {
                errors.push(format!("{key} must equal canonical value {expected}"));
            }
        }
        for (key, expected) in [
            ("server_cpu_limit", CANONICAL_SERVER_CPU_LIMIT),
            ("server_cpu_request", CANONICAL_SERVER_CPU_REQUEST),
            ("loadgen_cpu_limit", CANONICAL_LOADGEN_CPU_LIMIT),
        ] {
            if values.get(key).and_then(serde_json::Value::as_str) != Some(expected) {
                errors.push(format!("{key} must equal canonical value {expected}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate the complete governed cross-owner authority. The canonical producer performs three
    /// independently measured sweeps in one invocation; accepting a single favorable row would discard
    /// two thirds of that governed run.
    pub fn validate_release_rows(rows: &[LedgerRow], revision: &str) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if rows.len() != CANONICAL_SWEEPS.len() {
            errors.push(format!(
                "E2 scale authority must contain exactly {} rows, found {}",
                CANONICAL_SWEEPS.len(),
                rows.len()
            ));
        }

        let stable_fields = [
            "source_revision",
            "segment_max_latency_ms",
            "segment_target_bytes",
            "worker_threads_per_node",
            "server_cpu_limit",
            "server_cpu_request",
            "loadgen_cpu_limit",
            "cores",
            "kind_node_image",
            "queues_per_owner",
            "items_per_queue",
            "conns_per_queue",
            "pipe_size",
            "batch_size",
        ];
        let baseline = rows.first();
        let mut sweeps = std::collections::BTreeSet::new();
        for (index, row) in rows.iter().enumerate() {
            if let Err(row_errors) = validate_release_row(row, revision) {
                errors.extend(
                    row_errors
                        .into_iter()
                        .map(|error| format!("row {}: {error}", index + 1)),
                );
            }
            match row
                .measurements
                .values
                .get("sweep")
                .and_then(serde_json::Value::as_u64)
            {
                Some(sweep) if CANONICAL_SWEEPS.contains(&sweep) => {
                    if !sweeps.insert(sweep) {
                        errors.push(format!("row {} duplicates sweep {sweep}", index + 1));
                    }
                }
                Some(sweep) => errors.push(format!(
                    "row {} has non-canonical sweep {sweep}; expected one each of {:?}",
                    index + 1,
                    CANONICAL_SWEEPS
                )),
                None => errors.push(format!("row {} is missing numeric sweep", index + 1)),
            }

            if let Some(first) = baseline {
                if row.suite != first.suite
                    || row.command != first.command
                    || row.backend_profile != first.backend_profile
                    || row.seed != first.seed
                    || row.environment != first.environment
                    || row.pass_bar != first.pass_bar
                {
                    errors.push(format!(
                        "row {} changes stable producer topology or identity fields",
                        index + 1
                    ));
                }
                for field in stable_fields {
                    if row.measurements.values.get(field) != first.measurements.values.get(field) {
                        errors.push(format!(
                            "row {} changes stable producer field {field}",
                            index + 1
                        ));
                    }
                }
            }
        }
        let expected = CANONICAL_SWEEPS.into_iter().collect();
        if sweeps != expected {
            errors.push(format!(
                "E2 scale authority must contain unique sweeps {:?}, found {:?}",
                CANONICAL_SWEEPS, sweeps
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Judge the four E2 release bars from the MEASURED scale points. Pure: no IO, no process exit.
    pub fn evaluate_e2_bars(points: &[E2ScalePoint]) -> E2Verdict {
        let at = |n: usize| points.iter().find(|p| p.owners == n);
        let canonical_owners_present = CANONICAL_OWNER_COUNTS.iter().all(|&n| at(n).is_some());

        // Preserve worst-per-queue capacity observations, but gate only on finite positive progress. Absolute
        // speed depends on the declared host/topology and cannot make a portable release red.
        let worst_ingest_per_queue = points
            .iter()
            .map(|p| p.ingest_min_per_queue)
            .fold(f64::INFINITY, f64::min);
        let worst_drain_per_queue = points
            .iter()
            .map(|p| p.drain_min_per_queue)
            .fold(f64::INFINITY, f64::min);
        let floor_pass = points.iter().all(|point| {
            [
                point.ingest_aggregate,
                point.ingest_min_per_queue,
                point.drain_aggregate,
                point.drain_min_per_queue,
            ]
            .into_iter()
            .all(|value| value.is_finite() && value > 0.0)
                && point.queues_per_owner > 0
                && point.items_per_queue > 0
                && point.conns_per_queue > 0
        });

        let (
            nondecreasing,
            ratio_8_2,
            scale_pass,
            one_owner_confirmations,
            expected_confirmations,
            disjoint_pass,
        ) = if canonical_owners_present {
            let (p2, p4, p8) = (at(2).unwrap(), at(4).unwrap(), at(8).unwrap());
            let nondecreasing = p4.ingest_aggregate >= p2.ingest_aggregate
                && p8.ingest_aggregate >= p4.ingest_aggregate;
            let ratio_8_2 = p8.ingest_aggregate / p2.ingest_aggregate;
            let scale_pass = ratio_8_2.is_finite() && ratio_8_2 > 0.0;
            let one_owner_confirmations = p8.one_owner_confirmations;
            let expected_confirmations =
                expected_one_owner_confirmations(p8.owners, p8.queues_per_owner);
            let disjoint_pass = p8.queues_per_owner > 0
                && expected_confirmations > 0
                && one_owner_confirmations == expected_confirmations;
            (
                nondecreasing,
                ratio_8_2,
                scale_pass,
                one_owner_confirmations,
                expected_confirmations,
                disjoint_pass,
            )
        } else {
            (false, 0.0, false, 0, 0, false)
        };

        let bars_met = canonical_owners_present && scale_pass && floor_pass && disjoint_pass;

        E2Verdict {
            canonical_owners_present,
            nondecreasing,
            ratio_8_2,
            scale_pass,
            worst_ingest_per_queue,
            worst_drain_per_queue,
            floor_pass,
            one_owner_confirmations,
            expected_confirmations,
            disjoint_pass,
            bars_met,
        }
    }

    /// Build the TP-002 E2 verification-ledger row from the MEASURED scale points + tuning + the judged
    /// [`E2Verdict`]. The row's `evidence_tier`/`scale` are `release` IFF [`E2Verdict::bars_met`], else
    /// `smoke` (never a faked release row). The row shape is byte-for-byte compatible with the live evidence
    /// previously emitted by `fireweed-loadgen` (see `docs/perf/evidence/tp002-e2-multinode-kind-release.jsonl`).
    ///
    /// `points` MUST cover the canonical owner counts (2/4/8); the per-owner-count values are read by owner
    /// count.
    pub fn build_e2_row(
        points: &[E2ScalePoint],
        tuning: &E2Tuning,
        verdict: &E2Verdict,
    ) -> LedgerRow {
        let at = |n: usize| {
            points
                .iter()
                .find(|p| p.owners == n)
                .unwrap_or_else(|| panic!("build_e2_row needs a scale point for owners={n}"))
        };
        let tier = if verdict.bars_met { "release" } else { "smoke" };

        let values = BTreeMap::from([
            (
                "owners_2_ingest_aggregate_per_s".to_string(),
                serde_json::json!(at(2).ingest_aggregate.round()),
            ),
            (
                "owners_4_ingest_aggregate_per_s".to_string(),
                serde_json::json!(at(4).ingest_aggregate.round()),
            ),
            (
                "owners_8_ingest_aggregate_per_s".to_string(),
                serde_json::json!(at(8).ingest_aggregate.round()),
            ),
            (
                "owners_2_claim_finalize_aggregate_per_s".to_string(),
                serde_json::json!(at(2).drain_aggregate.round()),
            ),
            (
                "owners_4_claim_finalize_aggregate_per_s".to_string(),
                serde_json::json!(at(4).drain_aggregate.round()),
            ),
            (
                "owners_8_claim_finalize_aggregate_per_s".to_string(),
                serde_json::json!(at(8).drain_aggregate.round()),
            ),
            (
                "scale_out_8_vs_2_ingest_multiple".to_string(),
                serde_json::json!((verdict.ratio_8_2 * 100.0).round() / 100.0),
            ),
            (
                "scale_multiple_bar".to_string(),
                serde_json::json!(SCALE_MULTIPLE_BAR),
            ),
            (
                "ingest_aggregate_non_decreasing".to_string(),
                serde_json::json!(verdict.nondecreasing),
            ),
            (
                "worst_ingest_per_queue_per_s".to_string(),
                serde_json::json!(verdict.worst_ingest_per_queue.round()),
            ),
            (
                "worst_claim_finalize_per_queue_per_s".to_string(),
                serde_json::json!(verdict.worst_drain_per_queue.round()),
            ),
            (
                "e0_floor_per_s".to_string(),
                serde_json::json!(FLOOR_ITEMS_PER_SEC.round()),
            ),
            ("portable_gate".to_string(), serde_json::json!(true)),
            ("quiet_host_required".to_string(), serde_json::json!(false)),
            ("host_speed_gate".to_string(), serde_json::json!(false)),
            (
                "wall_clock_capacity_only".to_string(),
                serde_json::json!(true),
            ),
            (
                "one_owner_per_queue_confirmations".to_string(),
                serde_json::json!(verdict.one_owner_confirmations),
            ),
            (
                "queues_per_owner".to_string(),
                serde_json::json!(at(8).queues_per_owner),
            ),
            (
                "items_per_queue".to_string(),
                serde_json::json!(at(8).items_per_queue),
            ),
            (
                "conns_per_queue".to_string(),
                serde_json::json!(at(8).conns_per_queue),
            ),
            ("pipe_size".to_string(), serde_json::json!(tuning.pipe_size)),
            (
                "batch_size".to_string(),
                serde_json::json!(tuning.batch_size),
            ),
            (
                "segment_max_latency_ms".to_string(),
                serde_json::json!(tuning.segment_max_latency_ms),
            ),
            (
                "segment_target_bytes".to_string(),
                serde_json::json!(tuning.segment_target_bytes),
            ),
            (
                "worker_threads_per_node".to_string(),
                serde_json::json!(tuning.worker_threads_per_node),
            ),
            (
                "server_cpu_limit".to_string(),
                serde_json::json!(tuning.server_cpu_limit),
            ),
            (
                "server_cpu_request".to_string(),
                serde_json::json!(tuning.server_cpu_request),
            ),
            (
                "loadgen_cpu_limit".to_string(),
                serde_json::json!(tuning.loadgen_cpu_limit),
            ),
            (
                "kind_node_image".to_string(),
                serde_json::json!(tuning.kind_node_image),
            ),
            ("sweep".to_string(), serde_json::json!(tuning.sweep)),
            ("cores".to_string(), serde_json::json!(tuning.cores)),
            ("bars_met".to_string(), serde_json::json!(verdict.bars_met)),
            (
                "source_revision".to_string(),
                serde_json::json!(tuning.source_revision),
            ),
        ]);

        LedgerRow {
            suite: "performance_multi_node_object_log_e2_kind".into(),
            command: "scripts/perf/tp002-e2-kind.sh (fireweed-loadgen run -> emit-row; kind: CPU-limited server pods + lean in-cluster load Job)".into(),
            backend_profile: "object_log_sqlite_projection".into(),
            scale: tier.into(),
            seed: 0,
            environment: format!(
                "live multi-node ADR-008 owner cluster on a kind (Kubernetes-in-docker) cluster; \
                 {cores} cores; node image {node_image}; owner counts 2/4/8; each owner an independent \
                 fireweed-service Deployment(replicas=1)+Service on object_log_sqlite_projection in SEGMENTED \
                 group-commit mode (TD-004) with its own object-log root + sqlite projection on an emptyDir \
                 medium=Memory tmpfs, distinct FIREWEED_NODE_ID, disjoint FIREWEED_BOOTSTRAP_QUEUES, CPU \
                 request={req}/limit={lim}, {worker} worker threads; load driven by a LEAN, SEPARATED \
                 in-cluster Job (CPU limit {load}) speaking raw RESP pod->pod over Service ClusterIP to each \
                 owner; each queue driven by {conns} concurrent connections",
                cores = tuning.cores,
                node_image = tuning.kind_node_image,
                req = tuning.server_cpu_request,
                lim = tuning.server_cpu_limit,
                worker = tuning.worker_threads_per_node,
                load = tuning.loadgen_cpu_limit,
                conns = at(8).conns_per_queue,
            ),
            exit_status: 0,
            ac_ids: vec![],
            inv_ids: vec![],
            pass_bar: "E2: canonical 2/4/8 live owner topology; every measured queue makes ingest and claim/finalize progress; exact one-owner-per-queue isolation; wall-clock rates and scaling multiples are capacity evidence only".into(),
            evidence_tier: tier.into(),
            measurements: Measurements {
                tp002_evidence_ids: vec!["E2".into()],
                values,
            },
        }
    }
}

/// TP-002 E2 single-node queue-density release evidence.
pub mod density {
    use super::{LedgerRow, Measurements};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    pub const MIN_TOTAL_QUEUES: usize = 1001;
    pub const CANONICAL_HOT_ITEMS: u64 = 300_000;
    pub const CANONICAL_CONTROL_ITEMS: u64 = 10_000;
    pub const CANONICAL_HOT_CONNECTIONS: usize = 8;
    pub const CANONICAL_COLD_WORKERS: usize = 8;
    pub const CANONICAL_SERVER_WORKERS: usize = 4;
    pub const CANONICAL_SEED: u64 = 42;
    pub const CANONICAL_PROGRESS_BOUND_MS: u64 = 60_000;
    pub const MAX_SERVER_THREADS: usize = CANONICAL_SERVER_WORKERS;
    pub const MAX_SERVER_CONNECTIONS: usize = 32;
    pub const MAX_SERVER_TASKS: usize = 64;
    pub const MAX_SERVICE_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    pub const QUEUE_ACTIVITY_DEFINITION: &str = "a cold queue is active only when final XLEN is >0, and progress-eligible only when a non-empty claim/finalize operation started after HOT_START, completed before HOT_END, and completed before the item was reseeded; elapsed latency is capacity evidence only";
    pub const CANONICAL_PASS_BAR: &str = "exactly 1000 cold queues plus one hot queue on one live objectlog/sqlite node; exact accepted/claimed/finalized/pending reconciliation with zero lost or duplicate transitions; every cold queue claims/finalizes during active hot work with zero empty claims or progress violations; allocation-enforced shared resource caps; bracketed same-run measurements are complete and internally consistent; elapsed time, latency, and throughput are capacity evidence only; failover excluded (pqueue-0a1d4386)";

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct DensityMeasurement {
        pub hot_items: u64,
        pub control_items: u64,
        pub hot_sustain_windows: u64,
        pub hot_sustain_items: u64,
        pub hot_connections: usize,
        pub cold_worker_count: usize,
        pub configured_server_workers: usize,
        pub total_queues: usize,
        pub cold_queues_active: usize,
        pub cold_queues_progress_eligible: usize,
        pub cold_empty_claim_responses: usize,
        pub hot_accepted_items: u64,
        pub hot_claimed_items: u64,
        pub hot_finalized_items: u64,
        pub cold_accepted_items: u64,
        pub cold_claimed_items: u64,
        pub cold_finalized_items: u64,
        pub cold_pending_items: u64,
        pub lost_items: u64,
        pub duplicate_transitions: u64,
        pub queue_global_progress_violations: u64,
        pub baseline_before_ingest_per_s: f64,
        pub baseline_before_claim_finalize_per_s: f64,
        pub baseline_after_ingest_per_s: f64,
        pub baseline_after_claim_finalize_per_s: f64,
        pub baseline_control_ingest_per_s: f64,
        pub baseline_control_claim_finalize_per_s: f64,
        pub hot_ingest_per_s: f64,
        pub hot_claim_finalize_per_s: f64,
        pub max_progress_latency_ms: u64,
        pub progress_bound_ms: u64,
        pub noisy_neighbor_ingest_retention_pct: f64,
        pub noisy_neighbor_claim_retention_pct: f64,
        pub shared_worker_count: usize,
        pub shared_worker_limit: usize,
        pub connection_count: usize,
        pub connection_limit: usize,
        pub task_count: usize,
        pub task_limit: usize,
        pub memory_current_bytes: u64,
        pub memory_peak_bytes: u64,
        pub memory_limit_bytes: u64,
        pub memory_accounting_source: String,
        pub resource_enforcement_active: bool,
        pub hot_phase_resource_samples: usize,
        pub first_hot_resource_sample_unix_ms: u64,
        pub last_hot_resource_sample_unix_ms: u64,
        pub hot_phase_started_unix_ms: u64,
        pub hot_phase_ended_unix_ms: u64,
    }

    fn finite_positive(value: f64) -> bool {
        value.is_finite() && value > 0.0
    }

    fn approximately_equal(actual: f64, expected: f64) -> bool {
        finite_positive(actual)
            && finite_positive(expected)
            && (actual - expected).abs() <= expected.abs().max(1.0) * 0.001
    }

    fn harmonic_control(before: f64, after: f64) -> f64 {
        2.0 / (before.recip() + after.recip())
    }

    fn has_nonportable_host_gate(text: &str) -> bool {
        let normalized = text.to_ascii_lowercase();
        [
            "quiet host",
            "idle host",
            "items/s >=",
            "items/s >",
            "throughput >=",
            "latency <=",
            "latency <",
            "p95 <",
            "p99 <",
        ]
        .into_iter()
        .any(|needle| normalized.contains(needle))
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct DensityMetadata {
        pub command: String,
        pub revision: String,
        pub topology: String,
        pub hardware: String,
        pub seed: u64,
        pub duration_seconds: u64,
        pub queue_activity_definition: String,
        pub image_digest: String,
        pub clean_revision: bool,
    }

    pub fn bars_met(m: &DensityMeasurement) -> bool {
        let cold = m.total_queues.saturating_sub(1);
        m.hot_items == CANONICAL_HOT_ITEMS
            && m.control_items == CANONICAL_CONTROL_ITEMS
            && m.hot_sustain_windows > 0
            && m.hot_sustain_items == m.hot_items.checked_mul(m.hot_sustain_windows).unwrap_or(0)
            && m.hot_connections == CANONICAL_HOT_CONNECTIONS
            && m.cold_worker_count == CANONICAL_COLD_WORKERS
            && m.configured_server_workers == CANONICAL_SERVER_WORKERS
            && m.total_queues == MIN_TOTAL_QUEUES
            && m.cold_queues_active == cold
            && m.cold_queues_progress_eligible == cold
            && m.cold_empty_claim_responses == 0
            && m.hot_accepted_items
                == m.control_items
                    .checked_mul(2)
                    .and_then(|controls| controls.checked_add(m.hot_sustain_items))
                    .unwrap_or(0)
            && m.hot_claimed_items == m.hot_accepted_items
            && m.hot_finalized_items == m.hot_claimed_items
            && m.cold_accepted_items
                == m.cold_finalized_items
                    .checked_add(m.cold_pending_items)
                    .unwrap_or(0)
            && m.cold_claimed_items == m.cold_finalized_items
            && m.cold_pending_items == cold as u64
            && m.lost_items == 0
            && m.duplicate_transitions == 0
            && m.queue_global_progress_violations == 0
            && finite_positive(m.baseline_before_ingest_per_s)
            && finite_positive(m.baseline_before_claim_finalize_per_s)
            && finite_positive(m.baseline_after_ingest_per_s)
            && finite_positive(m.baseline_after_claim_finalize_per_s)
            && finite_positive(m.hot_ingest_per_s)
            && finite_positive(m.hot_claim_finalize_per_s)
            && approximately_equal(
                m.baseline_control_ingest_per_s,
                harmonic_control(
                    m.baseline_before_ingest_per_s,
                    m.baseline_after_ingest_per_s,
                ),
            )
            && approximately_equal(
                m.baseline_control_claim_finalize_per_s,
                harmonic_control(
                    m.baseline_before_claim_finalize_per_s,
                    m.baseline_after_claim_finalize_per_s,
                ),
            )
            && m.progress_bound_ms == CANONICAL_PROGRESS_BOUND_MS
            && m.noisy_neighbor_ingest_retention_pct.is_finite()
            && m.noisy_neighbor_ingest_retention_pct > 0.0
            && approximately_equal(
                m.noisy_neighbor_ingest_retention_pct,
                m.hot_ingest_per_s / m.baseline_control_ingest_per_s * 100.0,
            )
            && m.noisy_neighbor_claim_retention_pct.is_finite()
            && m.noisy_neighbor_claim_retention_pct > 0.0
            && approximately_equal(
                m.noisy_neighbor_claim_retention_pct,
                m.hot_claim_finalize_per_s / m.baseline_control_claim_finalize_per_s * 100.0,
            )
            && m.shared_worker_limit == MAX_SERVER_THREADS
            && m.shared_worker_count > 0
            && m.shared_worker_count <= m.shared_worker_limit
            && m.connection_limit == MAX_SERVER_CONNECTIONS
            && m.connection_count > 0
            && m.connection_count <= m.connection_limit
            && m.task_limit == MAX_SERVER_TASKS
            && m.task_count > 0
            && m.task_count <= m.task_limit
            && m.memory_current_bytes > 0
            && m.memory_current_bytes <= m.memory_peak_bytes
            && m.memory_peak_bytes <= m.memory_limit_bytes
            && m.memory_limit_bytes == MAX_SERVICE_MEMORY_BYTES
            && m.memory_accounting_source == "cgroup_v2"
            && m.resource_enforcement_active
            && m.hot_phase_started_unix_ms > 0
            && m.hot_phase_ended_unix_ms > m.hot_phase_started_unix_ms
    }

    pub fn build_release_row(m: &DensityMeasurement, meta: &DensityMetadata) -> LedgerRow {
        let pass = bars_met(m)
            && meta.seed == CANONICAL_SEED
            && meta.queue_activity_definition == QUEUE_ACTIVITY_DEFINITION
            && meta.clean_revision;
        let tier = if pass { "release" } else { "smoke" };
        let values = BTreeMap::from([
            ("bars_met".into(), serde_json::json!(pass)),
            ("hot_items".into(), serde_json::json!(m.hot_items)),
            ("control_items".into(), serde_json::json!(m.control_items)),
            (
                "hot_sustain_windows".into(),
                serde_json::json!(m.hot_sustain_windows),
            ),
            (
                "hot_sustain_items".into(),
                serde_json::json!(m.hot_sustain_items),
            ),
            (
                "hot_connections".into(),
                serde_json::json!(m.hot_connections),
            ),
            (
                "cold_worker_count".into(),
                serde_json::json!(m.cold_worker_count),
            ),
            (
                "configured_server_workers".into(),
                serde_json::json!(m.configured_server_workers),
            ),
            ("total_queues".into(), serde_json::json!(m.total_queues)),
            (
                "cold_queues_active".into(),
                serde_json::json!(m.cold_queues_active),
            ),
            (
                "cold_queues_progress_eligible".into(),
                serde_json::json!(m.cold_queues_progress_eligible),
            ),
            (
                "cold_empty_claim_responses".into(),
                serde_json::json!(m.cold_empty_claim_responses),
            ),
            (
                "hot_accepted_items".into(),
                serde_json::json!(m.hot_accepted_items),
            ),
            (
                "hot_claimed_items".into(),
                serde_json::json!(m.hot_claimed_items),
            ),
            (
                "hot_finalized_items".into(),
                serde_json::json!(m.hot_finalized_items),
            ),
            (
                "cold_accepted_items".into(),
                serde_json::json!(m.cold_accepted_items),
            ),
            (
                "cold_claimed_items".into(),
                serde_json::json!(m.cold_claimed_items),
            ),
            (
                "cold_finalized_items".into(),
                serde_json::json!(m.cold_finalized_items),
            ),
            (
                "cold_pending_items".into(),
                serde_json::json!(m.cold_pending_items),
            ),
            ("lost_items".into(), serde_json::json!(m.lost_items)),
            (
                "duplicate_transitions".into(),
                serde_json::json!(m.duplicate_transitions),
            ),
            (
                "queue_global_progress_violations".into(),
                serde_json::json!(m.queue_global_progress_violations),
            ),
            (
                "baseline_before_ingest_per_s".into(),
                serde_json::json!(m.baseline_before_ingest_per_s),
            ),
            (
                "baseline_before_claim_finalize_per_s".into(),
                serde_json::json!(m.baseline_before_claim_finalize_per_s),
            ),
            (
                "baseline_after_ingest_per_s".into(),
                serde_json::json!(m.baseline_after_ingest_per_s),
            ),
            (
                "baseline_after_claim_finalize_per_s".into(),
                serde_json::json!(m.baseline_after_claim_finalize_per_s),
            ),
            (
                "baseline_control_ingest_per_s".into(),
                serde_json::json!(m.baseline_control_ingest_per_s),
            ),
            (
                "baseline_control_claim_finalize_per_s".into(),
                serde_json::json!(m.baseline_control_claim_finalize_per_s),
            ),
            (
                "hot_ingest_per_s".into(),
                serde_json::json!(m.hot_ingest_per_s),
            ),
            (
                "hot_claim_finalize_per_s".into(),
                serde_json::json!(m.hot_claim_finalize_per_s),
            ),
            (
                "max_progress_latency_ms".into(),
                serde_json::json!(m.max_progress_latency_ms),
            ),
            (
                "progress_bound_ms".into(),
                serde_json::json!(m.progress_bound_ms),
            ),
            (
                "noisy_neighbor_ingest_retention_pct".into(),
                serde_json::json!(m.noisy_neighbor_ingest_retention_pct),
            ),
            (
                "noisy_neighbor_claim_retention_pct".into(),
                serde_json::json!(m.noisy_neighbor_claim_retention_pct),
            ),
            (
                "shared_worker_count".into(),
                serde_json::json!(m.shared_worker_count),
            ),
            (
                "shared_worker_limit".into(),
                serde_json::json!(m.shared_worker_limit),
            ),
            (
                "connection_count".into(),
                serde_json::json!(m.connection_count),
            ),
            (
                "connection_limit".into(),
                serde_json::json!(m.connection_limit),
            ),
            ("task_count".into(), serde_json::json!(m.task_count)),
            ("task_limit".into(), serde_json::json!(m.task_limit)),
            (
                "memory_current_bytes".into(),
                serde_json::json!(m.memory_current_bytes),
            ),
            (
                "memory_peak_bytes".into(),
                serde_json::json!(m.memory_peak_bytes),
            ),
            (
                "memory_limit_bytes".into(),
                serde_json::json!(m.memory_limit_bytes),
            ),
            (
                "memory_accounting_source".into(),
                serde_json::json!(m.memory_accounting_source),
            ),
            (
                "resource_enforcement_active".into(),
                serde_json::json!(m.resource_enforcement_active),
            ),
            (
                "hot_phase_resource_samples".into(),
                serde_json::json!(m.hot_phase_resource_samples),
            ),
            (
                "hot_phase_started_unix_ms".into(),
                serde_json::json!(m.hot_phase_started_unix_ms),
            ),
            (
                "hot_phase_ended_unix_ms".into(),
                serde_json::json!(m.hot_phase_ended_unix_ms),
            ),
            (
                "first_hot_resource_sample_unix_ms".into(),
                serde_json::json!(m.first_hot_resource_sample_unix_ms),
            ),
            (
                "last_hot_resource_sample_unix_ms".into(),
                serde_json::json!(m.last_hot_resource_sample_unix_ms),
            ),
            ("revision".into(), serde_json::json!(meta.revision)),
            (
                "duration_seconds".into(),
                serde_json::json!(meta.duration_seconds),
            ),
            (
                "queue_activity_definition".into(),
                serde_json::json!(meta.queue_activity_definition),
            ),
            ("failover_excluded".into(), serde_json::json!(true)),
            (
                "failover_reference".into(),
                serde_json::json!("pqueue-0a1d4386"),
            ),
            ("image_digest".into(), serde_json::json!(meta.image_digest)),
            (
                "clean_revision".into(),
                serde_json::json!(meta.clean_revision),
            ),
            ("portable_gate".into(), serde_json::json!(true)),
            ("quiet_host_required".into(), serde_json::json!(false)),
            ("host_speed_gate".into(), serde_json::json!(false)),
            ("wall_clock_capacity_only".into(), serde_json::json!(true)),
        ]);
        LedgerRow {
            suite: "queue_density_live_objectlog_sqlite_release".into(),
            command: meta.command.clone(),
            backend_profile: "object_log_sqlite_projection".into(),
            scale: tier.into(),
            seed: meta.seed,
            environment: format!("{}; hardware={}", meta.topology, meta.hardware),
            exit_status: 0,
            ac_ids: vec![],
            inv_ids: vec![],
            pass_bar: CANONICAL_PASS_BAR.into(),
            evidence_tier: tier.into(),
            measurements: Measurements {
                tp002_evidence_ids: vec!["E2".into()],
                values,
            },
        }
    }

    pub fn validate_release_row(row: &LedgerRow) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if row.suite != "queue_density_live_objectlog_sqlite_release" {
            errors.push("suite must be queue_density_live_objectlog_sqlite_release".into());
        }
        if row.backend_profile != "object_log_sqlite_projection" {
            errors.push("backend_profile must be object_log_sqlite_projection".into());
        }
        if row.scale != "release" || row.evidence_tier != "release" {
            errors.push("density row must be release tier and scale".into());
        }
        if row.measurements.tp002_evidence_ids.as_slice() != ["E2"] {
            errors.push("density row must carry exactly E2".into());
        }
        let values = &row.measurements.values;
        let number = |key: &str| values.get(key).and_then(serde_json::Value::as_f64);
        let integer = |key: &str| values.get(key).and_then(serde_json::Value::as_u64);
        let require_nonempty = |key: &str, errors: &mut Vec<String>| {
            if values
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty)
            {
                errors.push(format!("{key} is required"));
            }
        };
        if values.get("bars_met") != Some(&serde_json::Value::Bool(true)) {
            errors.push("bars_met must be true".into());
        }
        if has_nonportable_host_gate(&row.pass_bar) {
            errors.push(
                "pass_bar must not require a quiet host or absolute host-speed threshold".into(),
            );
        }
        if row.pass_bar != CANONICAL_PASS_BAR {
            errors.push("pass_bar must equal the governed canonical density pass bar".into());
        }
        let total = integer("total_queues").unwrap_or(0);
        if total != MIN_TOTAL_QUEUES as u64 {
            errors.push("total_queues must equal canonical 1001".into());
        }
        let cold = total.saturating_sub(1);
        if integer("cold_queues_active") != Some(cold) {
            errors.push("all cold queues must be active".into());
        }
        if integer("cold_queues_progress_eligible") != Some(cold) {
            errors.push("all cold queues must be progress eligible".into());
        }
        if integer("cold_empty_claim_responses") != Some(0) {
            errors.push("cold_empty_claim_responses must be zero".into());
        }
        let expected_hot = integer("control_items")
            .and_then(|control| control.checked_mul(2))
            .and_then(|control| control.checked_add(integer("hot_sustain_items")?));
        if integer("hot_accepted_items") != expected_hot
            || integer("hot_claimed_items") != expected_hot
            || integer("hot_finalized_items") != expected_hot
        {
            errors.push("hot accepted/claimed/finalized counts must reconcile exactly".into());
        }
        let cold_accepted = integer("cold_accepted_items");
        let cold_claimed = integer("cold_claimed_items");
        let cold_finalized = integer("cold_finalized_items");
        let cold_pending = integer("cold_pending_items");
        if cold_accepted.is_none_or(|accepted| {
            cold_finalized
                .and_then(|finalized| finalized.checked_add(cold_pending.unwrap_or(u64::MAX)))
                != Some(accepted)
        }) || cold_claimed != cold_finalized
            || cold_pending != Some(cold)
        {
            errors.push(
                "cold accepted/claimed/finalized/pending counts must reconcile exactly".into(),
            );
        }
        for key in [
            "lost_items",
            "duplicate_transitions",
            "queue_global_progress_violations",
        ] {
            if integer(key) != Some(0) {
                errors.push(format!("{key} must be zero"));
            }
        }
        for key in [
            "baseline_before_ingest_per_s",
            "baseline_before_claim_finalize_per_s",
            "baseline_after_ingest_per_s",
            "baseline_after_claim_finalize_per_s",
            "baseline_control_ingest_per_s",
            "baseline_control_claim_finalize_per_s",
            "hot_ingest_per_s",
            "hot_claim_finalize_per_s",
            "noisy_neighbor_ingest_retention_pct",
            "noisy_neighbor_claim_retention_pct",
        ] {
            if number(key).is_none_or(|value| !finite_positive(value)) {
                errors.push(format!("{key} must be finite and positive"));
            }
        }
        let comparisons = [
            (
                "baseline_control_ingest_per_s",
                harmonic_control(
                    number("baseline_before_ingest_per_s").unwrap_or(f64::NAN),
                    number("baseline_after_ingest_per_s").unwrap_or(f64::NAN),
                ),
            ),
            (
                "baseline_control_claim_finalize_per_s",
                harmonic_control(
                    number("baseline_before_claim_finalize_per_s").unwrap_or(f64::NAN),
                    number("baseline_after_claim_finalize_per_s").unwrap_or(f64::NAN),
                ),
            ),
            (
                "noisy_neighbor_ingest_retention_pct",
                number("hot_ingest_per_s").unwrap_or(f64::NAN)
                    / number("baseline_control_ingest_per_s").unwrap_or(f64::NAN)
                    * 100.0,
            ),
            (
                "noisy_neighbor_claim_retention_pct",
                number("hot_claim_finalize_per_s").unwrap_or(f64::NAN)
                    / number("baseline_control_claim_finalize_per_s").unwrap_or(f64::NAN)
                    * 100.0,
            ),
        ];
        for (key, expected) in comparisons {
            if number(key).is_none_or(|actual| !approximately_equal(actual, expected)) {
                errors.push(format!(
                    "{key} is inconsistent with the bracketed same-run measurements"
                ));
            }
        }
        if integer("progress_bound_ms") != Some(CANONICAL_PROGRESS_BOUND_MS) {
            errors.push("progress_bound_ms must record the canonical queue configuration".into());
        }
        if integer("max_progress_latency_ms").is_none() {
            errors.push("max_progress_latency_ms capacity observation is required".into());
        }
        for (key, expected) in [
            ("hot_items", CANONICAL_HOT_ITEMS),
            ("control_items", CANONICAL_CONTROL_ITEMS),
            ("hot_connections", CANONICAL_HOT_CONNECTIONS as u64),
            ("cold_worker_count", CANONICAL_COLD_WORKERS as u64),
            ("configured_server_workers", CANONICAL_SERVER_WORKERS as u64),
        ] {
            if integer(key) != Some(expected) {
                errors.push(format!("{key} must equal canonical {expected}"));
            }
        }
        let hot_items = integer("hot_items").unwrap_or(0);
        let sustain_windows = integer("hot_sustain_windows").unwrap_or(0);
        if sustain_windows == 0
            || integer("hot_sustain_items") != hot_items.checked_mul(sustain_windows)
        {
            errors.push(
                "hot_sustain_items must exactly reconcile canonical hot work across positive windows"
                    .into(),
            );
        }
        if row.seed != CANONICAL_SEED {
            errors.push(format!("seed must equal canonical {CANONICAL_SEED}"));
        }
        match (
            integer("hot_phase_started_unix_ms"),
            integer("hot_phase_ended_unix_ms"),
        ) {
            (Some(start), Some(end)) if start > 0 && end > start => {}
            _ => errors.push("hot-phase timestamps must be ordered and positive".into()),
        }
        if values
            .get("resource_enforcement_active")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            errors.push("resource_enforcement_active must be true".into());
        }
        for (key, expected) in [
            ("portable_gate", true),
            ("quiet_host_required", false),
            ("host_speed_gate", false),
            ("wall_clock_capacity_only", true),
        ] {
            if values.get(key).and_then(serde_json::Value::as_bool) != Some(expected) {
                errors.push(format!("{key} must be {expected}"));
            }
        }
        for (count, limit, governed_limit) in [
            (
                "shared_worker_count",
                "shared_worker_limit",
                MAX_SERVER_THREADS,
            ),
            (
                "connection_count",
                "connection_limit",
                MAX_SERVER_CONNECTIONS,
            ),
            ("task_count", "task_limit", MAX_SERVER_TASKS),
        ] {
            match (integer(count), integer(limit)) {
                (Some(c), Some(l)) if c > 0 && l == governed_limit as u64 && c <= l => {}
                _ => errors.push(format!(
                    "{count} must be bounded by governed {limit}={governed_limit}"
                )),
            }
        }
        match (
            integer("memory_current_bytes"),
            integer("memory_peak_bytes"),
            integer("memory_limit_bytes"),
        ) {
            (Some(current), Some(peak), Some(limit))
                if current > 0
                    && current <= peak
                    && peak <= limit
                    && limit == MAX_SERVICE_MEMORY_BYTES => {}
            _ => errors.push(format!(
                "service memory must be cgroup-bounded current<=peak<=limit={MAX_SERVICE_MEMORY_BYTES}"
            )),
        }
        if values
            .get("memory_accounting_source")
            .and_then(serde_json::Value::as_str)
            != Some("cgroup_v2")
        {
            errors.push("memory_accounting_source must be cgroup_v2".into());
        }
        require_nonempty("revision", &mut errors);
        if values
            .get("queue_activity_definition")
            .and_then(serde_json::Value::as_str)
            != Some(QUEUE_ACTIVITY_DEFINITION)
        {
            errors.push(
                "queue_activity_definition must record HOT_START claim-start semantics".into(),
            );
        }
        if integer("duration_seconds").is_none_or(|v| v == 0) {
            errors.push("duration_seconds must be positive".into());
        }
        if values.get("failover_excluded") != Some(&serde_json::Value::Bool(true)) {
            errors.push("failover_excluded must be true".into());
        }
        if values
            .get("failover_reference")
            .and_then(serde_json::Value::as_str)
            != Some("pqueue-0a1d4386")
        {
            errors.push("failover_reference must be pqueue-0a1d4386".into());
        }
        if row.command.trim().is_empty() {
            errors.push("command is required".into());
        }
        if row.command != "scripts/perf/tp002-e2-density-kind.sh" {
            errors.push("command must be scripts/perf/tp002-e2-density-kind.sh".into());
        }
        let revision = values
            .get("revision")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
            errors.push("revision must be a full 40-character Git SHA".into());
        }
        let digest = values
            .get("image_digest")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if digest.len() != 71
            || !digest.starts_with("sha256:")
            || !digest[7..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            errors.push("image_digest must be a sha256 digest".into());
        }
        if values.get("clean_revision") != Some(&serde_json::Value::Bool(true)) {
            errors.push("clean_revision must be true".into());
        }
        if !row.environment.contains("live one-node kind deployment")
            || !row.environment.contains("objectlog/sqlite")
            || !row.environment.contains("cores")
            || !row.environment.contains("GiB RAM")
        {
            errors.push(
                "environment must record live one-node objectlog/sqlite topology and hardware"
                    .into(),
            );
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// TP-002 **E3 cost model** (ADR-001 "Napkin Cost Comparison" → release evidence).
///
/// ADR-001 asserts, *directionally*, that the `object_log_sqlite_projection` backend has a lower
/// $/command than an always-on relational authority at high volume, because batched object-storage commits
/// (request-priced PUTs + cheap `$/GB-month` storage, **no** per-I/O or provisioned-IOPS charge) beat a
/// provisioned database instance that must hold the resident backlog and sustain the high-churn
/// `SKIP LOCKED` claim index. This module turns that direction into a reproducible, fixture-tested
/// **calculation**: it scales the REAL E3 measured object/segment counts to a billion commands, prices
/// them against cited inputs, prices the `postgres_native` baseline (instance-hours at the measured E0
/// throughput + storage + provisioned IOPS), and returns a structured [`CostComparison`].
///
/// It is a PURE function of its inputs — no IO, no process exit — so the comparison is unit-testable from a
/// fixture and the calculator can be shown to RESPOND to its inputs (crank a price until the crossover) rather
/// than being hard-wired to a conclusion.
///
/// ## Apples-to-apples (the honesty bar)
///
/// `object_log_sqlite_projection` ALSO runs a compute node (it batches commands into segments and projects
/// them into SQLite), so this is NOT "free S3 vs a paid DB". Both sides are charged compute for the same
/// always-on billing window. The legitimate, modelled win is two-fold and each is a separate, inspectable
/// line item:
/// 1. **Durable storage + I/O**: the durable log lives on object storage (`$/GB-month` + request-priced
///    PUTs, *no per-I/O charge*) instead of DB storage + **provisioned IOPS** sized for the claim-index
///    churn (the MVCC-bloat finding in `docs/perf/tp002-e0e1-postgres-release-10m.md` documents how
///    IOPS-bound that path is).
/// 2. **Node sizing**: the object-log node can be smaller/cheaper than the IOPS-bound claim authority — but
///    this is exposed as a separate price input so a reviewer can set both nodes equal and confirm the win
///    survives on the storage/I/O term alone.
pub mod cost {
    use super::{LedgerRow, Measurements};
    use std::collections::BTreeMap;

    /// One billion — the command count every cost figure is normalized to (`$/billion-commands`).
    pub const BILLION: f64 = 1_000_000_000.0;
    /// Hours in a 30-day month (`30 * 24 + 10`… AWS bills `$/GB-month` against 730 hours).
    pub const HOURS_PER_MONTH: f64 = 730.0;
    /// Bytes per **decimal** GB — cloud `$/GB-month` and `$/GB` pricing is decimal (10^9), not GiB.
    pub const BYTES_PER_GB: f64 = 1_000_000_000.0;

    /// Cited price inputs (US-East-1). Defaults come from [`PriceInputs::adr_001_us_east_1`]; every default is
    /// traceable to ADR-001's "Napkin Cost Comparison" cited offer-file set except the EBS provisioned-IOPS
    /// unit price, which ADR-001 does not cite and is noted as such ([`Self::iops_source`]).
    #[derive(Debug, Clone, PartialEq)]
    pub struct PriceInputs {
        /// S3 Standard storage, `$/GB-month`.
        pub s3_storage_per_gb_month: f64,
        /// S3 PUT/COPY/POST/LIST, `$/1000 requests`.
        pub s3_put_per_1k: f64,
        /// S3 GET, `$/1000 requests`.
        pub s3_get_per_1k: f64,
        /// S3 DELETE/CANCEL, `$/1000 requests`. S3 Standard prices these at zero; keep the input explicit so
        /// request-accounting rows can show that deletes are tracked rather than ignored.
        pub s3_delete_per_1k: f64,
        /// The `postgres_native` provisioned DB instance, `$/hour` (the always-on claim authority).
        pub pg_instance_per_hour: f64,
        /// DB storage, `$/GB-month`.
        pub pg_storage_per_gb_month: f64,
        /// Provisioned IOPS, `$/IOPS-month` (one provisioned I/O operation per second for a month).
        pub pg_iops_per_month_each: f64,
        /// The `object_log_sqlite_projection` compute node, `$/hour` (it batches + projects; can be smaller).
        pub objectlog_node_per_hour: f64,
        /// Provenance of the S3/DB instance/storage prices.
        pub instance_source: &'static str,
        /// Provenance of the provisioned-IOPS unit price (NOT cited by ADR-001 — stated honestly).
        pub iops_source: &'static str,
    }

    impl PriceInputs {
        /// ADR-001's cited US-East-1 inputs (S3 Standard; Aurora PostgreSQL `db.r7g.large` standard as the
        /// `postgres_native` instance; EC2 `i4i.large` — NVMe-backed, suits the SQLite projection + segment
        /// buffer — as the object-log node). The provisioned-IOPS unit price is AWS EBS `io2` first-tier,
        /// which ADR-001 does not cite; it is flagged in [`Self::iops_source`].
        pub fn adr_001_us_east_1() -> Self {
            PriceInputs {
                s3_storage_per_gb_month: 0.023,
                s3_put_per_1k: 0.005,
                s3_get_per_1k: 0.0004,
                s3_delete_per_1k: 0.0,
                pg_instance_per_hour: 0.276,
                pg_storage_per_gb_month: 0.10,
                pg_iops_per_month_each: 0.065,
                objectlog_node_per_hour: 0.172,
                instance_source: "ADR-001 Napkin Cost Comparison, US-East-1: AWS S3 pricing (AmazonS3 offer file pub. \
                     2026-05-28); Aurora PostgreSQL db.r7g.large standard $0.276/hr + $0.10/GB-mo storage \
                     (AmazonRDS offer file pub. 2026-06-05); EC2 i4i.large $0.172/hr (AmazonEC2 offer file \
                     pub. 2026-06-04)",
                iops_source: "AWS EBS io2 provisioned-IOPS first tier $0.065/IOPS-month (AWS EBS pricing page, \
                     accessed 2026-06-29) — NOT cited by ADR-001; stated as the one non-ADR price input",
            }
        }
    }

    /// Measured object-log counts the cost scales to a billion commands. The headline fixture uses the REAL
    /// E3 numbers from the current governed release ledger; the production-fill
    /// constructors model segments filled to their byte target (the E3 segments were latency-bound and small,
    /// which OVER-states PUT cost — see [`Self::e3_size_dominant`]).
    #[derive(Debug, Clone, PartialEq)]
    pub struct ObjectLogCounts {
        /// A short label for the scenario (appears in the artifact's sensitivity table).
        pub label: String,
        /// Commands committed in the measured (or modelled) sample.
        pub commands: f64,
        /// Objects PUT for those commands (segment object + manifest object per seal in E3 ⇒ 2/segment).
        pub objects_put: f64,
        /// Segments sealed for those commands.
        pub segments_sealed: f64,
        /// Billable PUT-class requests observed through the live blob-store seam.
        pub put_requests: f64,
        /// Billable GET requests observed through the live blob-store seam.
        pub get_requests: f64,
        /// Billable LIST requests observed through the live blob-store seam.
        pub list_requests: f64,
        /// DELETE requests observed through the live blob-store seam.
        pub delete_requests: f64,
        pub request_bytes: f64,
        pub response_bytes: f64,
        /// Projection-specific rebuild mode proven by the source row.
        pub recovery_mode: RecoveryMode,
        /// Durable commands represented by the measured 10M recovery run.
        pub recovery_commands: f64,
        pub recovery_put_requests: f64,
        pub recovery_get_requests: f64,
        pub recovery_list_requests: f64,
        pub recovery_delete_requests: f64,
        pub recovery_request_bytes: f64,
        pub recovery_response_bytes: f64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RecoveryMode {
        SnapshotTail,
        FullGenesis,
    }

    impl ObjectLogCounts {
        /// REAL E3 size-dominant config (`target_bytes=4096, max_latency=1000ms`): 2048 commands → 34 segments,
        /// 68 objects. These segments sealed mostly on the latency cap with small synthetic commands, so the
        /// objects-per-command ratio is HIGHER (worse for cost) than a throughput-saturated production run that
        /// fills segments to their byte target — i.e. this is the pessimistic-but-measured case.
        pub fn e3_size_dominant() -> Self {
            ObjectLogCounts {
                label: "E3 measured (size-dominant: 4 KiB target / 1000 ms cap)".into(),
                commands: 2048.0,
                objects_put: 68.0,
                segments_sealed: 34.0,
                put_requests: 68.0,
                get_requests: 0.0,
                list_requests: 0.0,
                delete_requests: 0.0,
                request_bytes: 0.0,
                response_bytes: 0.0,
                recovery_mode: RecoveryMode::SnapshotTail,
                recovery_commands: 0.0,
                recovery_put_requests: 0.0,
                recovery_get_requests: 0.0,
                recovery_list_requests: 0.0,
                recovery_delete_requests: 0.0,
                recovery_request_bytes: 0.0,
                recovery_response_bytes: 0.0,
            }
        }

        /// REAL E3 latency-dominant config (`target_bytes=8 MiB, max_latency=50ms`): 2048 commands → 50 segments,
        /// 100 objects. The tighter 50 ms cap seals more, smaller segments ⇒ the highest measured PUT-per-command.
        pub fn e3_latency_dominant() -> Self {
            ObjectLogCounts {
                label: "E3 measured (latency-dominant: 8 MiB target / 50 ms cap)".into(),
                commands: 2048.0,
                objects_put: 100.0,
                segments_sealed: 50.0,
                put_requests: 100.0,
                get_requests: 0.0,
                list_requests: 0.0,
                delete_requests: 0.0,
                request_bytes: 0.0,
                response_bytes: 0.0,
                recovery_mode: RecoveryMode::SnapshotTail,
                recovery_commands: 0.0,
                recovery_put_requests: 0.0,
                recovery_get_requests: 0.0,
                recovery_list_requests: 0.0,
                recovery_delete_requests: 0.0,
                recovery_request_bytes: 0.0,
                recovery_response_bytes: 0.0,
            }
        }

        /// Production-fill model: segments filled to `target_bytes` with `bytes_per_command`-sized commands,
        /// `objects_per_segment` objects per seal (2 in E3: one segment object + one manifest object). This is
        /// what a throughput-saturated owner produces (ADR-001's "16 MiB segments ⇒ <\$2 in PUTs" case).
        pub fn filled(
            label: impl Into<String>,
            target_bytes: f64,
            bytes_per_command: f64,
            objects_per_segment: f64,
        ) -> Self {
            let commands_per_segment = (target_bytes / bytes_per_command).max(1.0);
            let segments = 1000.0; // arbitrary sample; only the ratios objects/cmd & seg/cmd are used
            ObjectLogCounts {
                label: label.into(),
                commands: commands_per_segment * segments,
                objects_put: objects_per_segment * segments,
                segments_sealed: segments,
                put_requests: objects_per_segment * segments,
                get_requests: 0.0,
                list_requests: 0.0,
                delete_requests: 0.0,
                request_bytes: 0.0,
                response_bytes: 0.0,
                recovery_mode: RecoveryMode::SnapshotTail,
                recovery_commands: 0.0,
                recovery_put_requests: 0.0,
                recovery_get_requests: 0.0,
                recovery_list_requests: 0.0,
                recovery_delete_requests: 0.0,
                recovery_request_bytes: 0.0,
                recovery_response_bytes: 0.0,
            }
        }

        /// Mean commands per sealed segment (segment fill, for display).
        pub fn commands_per_segment(&self) -> f64 {
            self.commands / self.segments_sealed
        }
    }

    /// Workload assumptions for the object-granularity sensitivity model. Unlike [`ObjectLogCounts`], these
    /// are not measured release evidence: they make the relationship between arrival rate, encoded command
    /// size, the byte target, and the latency seal explicit for capacity and cost planning.
    #[derive(Debug, Clone, PartialEq)]
    pub struct GranularityAssumptions {
        pub label: String,
        /// Independently segmented active queues with this same per-queue workload shape.
        pub active_queue_count: f64,
        /// Arrival rate for each queue, because queues cannot share a segment.
        pub command_rate_per_s: f64,
        /// Fixed downstream primitive batch size for this deterministic sensitivity case.
        pub input_batch_commands: f64,
        pub encoded_command_bytes: f64,
        pub target_segment_bytes: f64,
        pub max_latency_ms: f64,
        /// Lifetime recovery-index entries already present at the start of the billing window.
        pub starting_recovery_index_entries: u64,
        pub billing_window_hours: f64,
        pub recovery_window_hours: f64,
    }

    /// Derived steady-arrival object-log shape. Every field is a deterministic function of
    /// [`GranularityAssumptions`] and the cited S3 PUT price.
    #[derive(Debug, Clone, PartialEq)]
    pub struct GranularityEstimate {
        pub seal_trigger: &'static str,
        pub put_requests_per_segment: f64,
        pub ending_recovery_index_height: u8,
        pub commands_per_segment: f64,
        pub segment_bytes: f64,
        pub fill_ratio: f64,
        pub commands_per_billing_window: f64,
        pub segments_per_billing_window: f64,
        pub put_requests_per_billing_window: f64,
        pub ingress_gb_per_billing_window: f64,
        pub retained_log_gb: f64,
        pub payload_storage_usd_per_month: f64,
        pub put_usd_per_billing_window: f64,
        pub put_usd_per_billion_commands: f64,
    }

    fn recovery_index_height(entries: u64) -> u8 {
        let mut height = 0_u8;
        let mut capacity = 64_u128;
        while u128::from(entries) > capacity && height < 10 {
            height += 1;
            capacity = capacity.saturating_mul(64);
        }
        height
    }

    /// Integrate successful-seal PUT amplification across fanout-64 recovery-index height transitions.
    /// Entry one writes segment + candidate + head + the first leaf (4 PUTs). Later appends write those
    /// three base objects, one COW node per level, and one retirement marker: `5 + resulting height` on an
    /// ordinary append. A root-height transition reuses the old root and has no retirement marker.
    fn recovery_index_put_requests(start: u64, appends: u64) -> Result<(u64, u8), &'static str> {
        let end = start
            .checked_add(appends)
            .ok_or("recovery-index entry count overflow")?;
        let mut total = 0_u128;
        if start == 0 && appends > 0 {
            total += 4;
        }
        let first = start.saturating_add(1).max(2);
        if first <= end {
            let mut lower = 2_u64;
            let mut upper = 64_u64;
            for height in 0_u8..=10 {
                let bucket_start = first.max(lower);
                let bucket_end = end.min(upper);
                if bucket_start <= bucket_end {
                    let count = bucket_end - bucket_start + 1;
                    total += u128::from(count) * u128::from(5_u8 + height);
                }
                if upper == u64::MAX || upper >= end {
                    break;
                }
                lower = upper.saturating_add(1);
                upper = upper.saturating_mul(64);
            }

            // A root-height transition reuses the old full root as a child. It writes the new right-edge
            // path and new root but supersedes no node, so that one append emits no retirement marker.
            let mut full_capacity = 64_u64;
            while full_capacity < end {
                let transition_entry = full_capacity.saturating_add(1);
                if transition_entry >= first && transition_entry <= end {
                    total = total.saturating_sub(1);
                }
                let next = full_capacity.saturating_mul(64);
                if next == full_capacity {
                    break;
                }
                full_capacity = next;
            }
        }
        let total = u64::try_from(total).map_err(|_| "PUT request count exceeds u64")?;
        Ok((total, recovery_index_height(end)))
    }

    /// Estimate steady-state group-commit granularity. The first command starts the latency clock; a segment
    /// contains the smaller of (a) commands that fit under the byte target and (b) commands arriving before
    /// the latency bound. A command larger than the target is still one legal segment and reports fill > 1.
    pub fn estimate_granularity(
        assumptions: &GranularityAssumptions,
        prices: &PriceInputs,
    ) -> Result<GranularityEstimate, &'static str> {
        let positive_finite = |value: f64| value.is_finite() && value > 0.0;
        if !positive_finite(assumptions.active_queue_count)
            || !positive_finite(assumptions.command_rate_per_s)
            || !positive_finite(assumptions.input_batch_commands)
            || !positive_finite(assumptions.encoded_command_bytes)
            || !positive_finite(assumptions.target_segment_bytes)
            || !positive_finite(assumptions.max_latency_ms)
            || !positive_finite(assumptions.billing_window_hours)
            || !positive_finite(assumptions.recovery_window_hours)
            || !positive_finite(prices.s3_put_per_1k)
        {
            return Err("granularity inputs and S3 PUT price must be finite and positive");
        }

        let input_batch_bytes =
            assumptions.input_batch_commands * assumptions.encoded_command_bytes;
        let size_limited_commands = (assumptions.target_segment_bytes / input_batch_bytes)
            .ceil()
            .max(1.0)
            * assumptions.input_batch_commands;
        let latency_limited_commands = (assumptions.command_rate_per_s
            * assumptions.max_latency_ms
            / 1000.0
            / assumptions.input_batch_commands)
            .ceil()
            .max(1.0)
            * assumptions.input_batch_commands;
        let commands_per_segment = size_limited_commands.min(latency_limited_commands);
        let seal_trigger = if size_limited_commands <= latency_limited_commands {
            "size"
        } else {
            "latency"
        };
        let segment_bytes = commands_per_segment * assumptions.encoded_command_bytes;
        let seconds = assumptions.billing_window_hours * 3600.0;
        let commands_per_queue_window = assumptions.command_rate_per_s * seconds;
        let commands_per_billing_window =
            commands_per_queue_window * assumptions.active_queue_count;
        let segments_per_queue_window = (commands_per_queue_window / commands_per_segment).ceil();
        if segments_per_queue_window > u64::MAX as f64 {
            return Err("per-queue segment count exceeds u64");
        }
        let (put_requests_per_queue_window, ending_recovery_index_height) =
            recovery_index_put_requests(
                assumptions.starting_recovery_index_entries,
                segments_per_queue_window as u64,
            )?;
        let segments_per_billing_window =
            segments_per_queue_window * assumptions.active_queue_count;
        let put_requests_per_segment =
            put_requests_per_queue_window as f64 / segments_per_queue_window;
        let put_requests_per_billing_window =
            put_requests_per_queue_window as f64 * assumptions.active_queue_count;
        let put_usd_per_billing_window =
            put_requests_per_billing_window / 1000.0 * prices.s3_put_per_1k;
        let put_usd_per_billion_commands =
            put_requests_per_segment / commands_per_segment * BILLION / 1000.0
                * prices.s3_put_per_1k;

        let retained_log_gb = assumptions.command_rate_per_s
            * assumptions.active_queue_count
            * assumptions.recovery_window_hours
            * 3600.0
            * assumptions.encoded_command_bytes
            / BYTES_PER_GB;
        Ok(GranularityEstimate {
            seal_trigger,
            put_requests_per_segment,
            ending_recovery_index_height,
            commands_per_segment,
            segment_bytes,
            fill_ratio: segment_bytes / assumptions.target_segment_bytes,
            commands_per_billing_window,
            segments_per_billing_window,
            put_requests_per_billing_window,
            ingress_gb_per_billing_window: commands_per_billing_window
                * assumptions.encoded_command_bytes
                / BYTES_PER_GB,
            retained_log_gb,
            payload_storage_usd_per_month: retained_log_gb * prices.s3_storage_per_gb_month,
            put_usd_per_billing_window,
            put_usd_per_billion_commands,
        })
    }

    /// Version tag for the cited price bundle. Validators reject rows from any older/different bundle.
    pub const PRICE_SOURCE_REVISION: &str = "aws-us-east-1-offers-2026-06-29";
    const E3_PROFILES: [&str; 2] = [
        "object_log_inmemory_projection",
        "object_log_sqlite_projection",
    ];
    const E3_BOUNDS: [&str; 4] = ["1ms", "5ms", "20ms", "100ms"];

    /// One release E3 profile/bound translated into measured cost inputs.
    #[derive(Debug, Clone, PartialEq)]
    pub struct ReleaseCostInput {
        pub backend_profile: String,
        pub bound: String,
        pub counts: ObjectLogCounts,
        pub source_command: String,
        pub source_environment: String,
        pub source_revision: String,
    }

    fn value_u64(row: &LedgerRow, key: &str, errors: &mut Vec<String>) -> Option<u64> {
        match row
            .measurements
            .values
            .get(key)
            .and_then(serde_json::Value::as_u64)
        {
            Some(value) => Some(value),
            None => {
                errors.push(format!(
                    "profile {} missing measured {key}",
                    row.backend_profile
                ));
                None
            }
        }
    }

    fn value_f64(row: &LedgerRow, key: &str, errors: &mut Vec<String>) -> Option<f64> {
        match row
            .measurements
            .values
            .get(key)
            .and_then(serde_json::Value::as_f64)
            .filter(|value| value.is_finite())
        {
            Some(value) => Some(value),
            None => {
                errors.push(format!(
                    "profile {} missing finite measured {key}",
                    row.backend_profile
                ));
                None
            }
        }
    }

    fn value_true(row: &LedgerRow, key: &str) -> bool {
        row.measurements
            .values
            .get(key)
            .and_then(serde_json::Value::as_bool)
            == Some(true)
    }

    fn has_nonportable_host_gate(row: &LedgerRow) -> bool {
        let text = format!("{} {}", row.environment, row.pass_bar).to_ascii_lowercase();
        let quiet_host_gate = ["quiet host", "quiet-host", "quiet window", "idle host"]
            .iter()
            .any(|phrase| text.contains(phrase));
        let absolute_speed_gate = [
            "throughput >=",
            "throughput >=",
            "throughput floor",
            "items/s floor",
            "sub-second p95",
            "sub-second p99",
        ]
        .iter()
        .any(|phrase| text.contains(phrase));
        quiet_host_gate
            || absolute_speed_gate
            || !value_true(row, "portable_gate")
            || value_true(row, "quiet_host_required")
            || value_true(row, "host_speed_gate")
    }

    /// Validate governed live E3 rows and extract every profile/bound's measured request counters.
    pub fn release_cost_inputs(rows: &[LedgerRow]) -> Result<Vec<ReleaseCostInput>, Vec<String>> {
        let mut errors = Vec::new();
        let mut inputs = Vec::new();
        for profile in E3_PROFILES {
            let matching = rows
                .iter()
                .filter(|row| row.backend_profile == profile)
                .collect::<Vec<_>>();
            let [row] = matching.as_slice() else {
                if matching.is_empty() {
                    errors.push(format!("missing profile {profile}"));
                } else {
                    errors.push(format!(
                        "profile {profile} has {} source rows; expected exactly one",
                        matching.len()
                    ));
                }
                continue;
            };
            if row.suite != "performance_object_log_e3_live_tests"
                || row.exit_status != 0
                || row.measurements.tp002_evidence_ids != ["E3"]
            {
                errors.push(format!(
                    "profile {profile} has invalid source identity/status/E3 linkage"
                ));
            }
            if row.backend_profile != profile {
                errors.push(format!("missing profile {profile}"));
                continue;
            }
            if row.scale != "release" || row.evidence_tier != "release" {
                errors.push(format!("profile {profile} cost source is not release-tier"));
            }
            if !value_true(row, "bars_met") {
                errors.push(format!("profile {profile} bars_met is not true"));
            }
            if has_nonportable_host_gate(row) {
                errors.push(format!(
                    "profile {profile} uses a quiet-host or absolute host-speed gate"
                ));
            }
            let topology_id = row
                .measurements
                .values
                .get("storage_topology_id")
                .and_then(serde_json::Value::as_str);
            let topology_description = row
                .measurements
                .values
                .get("storage_topology_description")
                .and_then(serde_json::Value::as_str);
            let stable_topology_id = topology_id.is_some_and(|value| {
                let mut bytes = value.bytes();
                (3..=128).contains(&value.len())
                    && bytes
                        .next()
                        .is_some_and(|byte| byte.is_ascii_alphanumeric())
                    && bytes.all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                    })
            });
            let authority_mode = row
                .measurements
                .values
                .get("storage_authority_mode")
                .and_then(serde_json::Value::as_str);
            if !stable_topology_id
                || topology_description.is_none_or(|value| value.trim().is_empty())
                || row
                    .measurements
                    .values
                    .get("storage_durability_claim")
                    .and_then(serde_json::Value::as_str)
                    != Some("excluded")
                || authority_mode != Some("native-create-only")
            {
                errors.push(format!(
                    "profile {profile} lacks a declared provider-neutral S3 topology, supported authority mode, or durability exclusion"
                ));
            }
            let source_revision = row
                .measurements
                .values
                .get("source_revision")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if source_revision.len() != 40
                || !source_revision.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                errors.push(format!(
                    "profile {profile} lacks an exact committed source revision"
                ));
            }
            if !value_true(row, "recovery_bar_met") {
                errors.push(format!(
                    "profile {profile} recovery bar failed or is missing"
                ));
            }
            if value_u64(row, "recovery_resident", &mut errors) != Some(10_000_000) {
                errors.push(format!("profile {profile} recovery is not the 10M shape"));
            }
            let snapshot = value_true(row, "recovery_snapshot_used");
            let start = value_u64(row, "recovery_start_seq", &mut errors);
            let tail = value_u64(row, "recovery_tail_replayed", &mut errors);
            let total = value_u64(row, "recovery_total_commands", &mut errors);
            let command_count = value_u64(row, "recovery_command_count", &mut errors);
            let replay_progress = row
                .measurements
                .values
                .get("recovery_replay_progress_samples")
                .and_then(serde_json::Value::as_array)
                .and_then(|samples| {
                    samples
                        .iter()
                        .map(serde_json::Value::as_u64)
                        .collect::<Option<Vec<_>>>()
                });
            let recovery_puts = value_u64(row, "recovery_store_put_requests", &mut errors);
            let recovery_gets = value_u64(row, "recovery_store_get_requests", &mut errors);
            let recovery_lists = value_u64(row, "recovery_store_list_requests", &mut errors);
            let recovery_deletes = value_u64(row, "recovery_store_delete_requests", &mut errors);
            let recovery_request_bytes =
                value_u64(row, "recovery_store_request_bytes", &mut errors);
            let recovery_response_bytes =
                value_u64(row, "recovery_store_response_bytes", &mut errors);
            let recovery_mode_ok = if profile == "object_log_sqlite_projection" {
                snapshot
                    && start.is_some_and(|value| value > 0)
                    && tail.zip(total).is_some_and(|(t, n)| t > 0 && t < n)
            } else {
                !snapshot && start == Some(0) && tail.zip(total).is_some_and(|(t, n)| t == n)
            };
            if !recovery_mode_ok {
                errors.push(format!(
                    "profile {profile} recovery mode does not match projection contract"
                ));
            }
            let command_range_exact = start.zip(tail).zip(total).zip(command_count).is_some_and(
                |(((start, tail), total), command_count)| {
                    start.checked_add(tail) == Some(total) && total == command_count
                },
            );
            let replay_progress_exact = replay_progress.as_ref().is_some_and(|samples| {
                samples.len() >= 2
                    && samples.windows(2).all(|pair| pair[0] <= pair[1])
                    && samples.first().copied() == start
                    && samples.last().copied() == total
            });
            if !command_range_exact || !replay_progress_exact {
                errors.push(format!(
                    "profile {profile} recovery cost source lacks an exact command range and replay progress endpoints"
                ));
            }
            let load_segments = value_u64(row, "recovery_load_segments_sealed", &mut errors);
            let load_size = value_u64(row, "recovery_load_size_triggered_seals", &mut errors);
            let load_latency = value_u64(row, "recovery_load_latency_triggered_seals", &mut errors);
            let load_forced = value_u64(row, "recovery_load_forced_seals", &mut errors);
            let load_rollover = value_u64(row, "recovery_load_rollover_seals", &mut errors);
            let load_batch_sum =
                value_u64(row, "recovery_load_group_commit_batch_sum", &mut errors);
            let load_command_count = value_u64(row, "recovery_load_command_count", &mut errors);
            let load_segment_bytes = value_u64(row, "recovery_load_segment_bytes", &mut errors);
            let load_mean = value_f64(row, "recovery_load_mean_commands_per_segment", &mut errors);
            let load_max = value_u64(row, "recovery_load_max_commands_per_segment", &mut errors);
            let load_shape_exact = load_size.zip(load_latency).zip(load_segments).is_some_and(
                |((size, latency), segments)| {
                    size > latency && latency <= 1 && size.checked_add(latency) == Some(segments)
                },
            ) && load_forced == Some(0)
                && load_rollover == Some(0)
                && load_batch_sum == load_command_count
                && load_command_count
                    == command_count.and_then(|commands| {
                        commands.checked_sub(u64::from(profile == "object_log_sqlite_projection"))
                    })
                && load_segment_bytes.is_some_and(|bytes| bytes > 0)
                && load_max
                    .zip(load_command_count)
                    .is_some_and(|(max, commands)| max > 1 && max <= commands)
                && load_mean
                    .zip(load_command_count)
                    .zip(load_segments)
                    .is_some_and(|((mean, commands), segments)| {
                        segments > 0
                            && mean > 1.0
                            && (mean - commands as f64 / segments as f64).abs() <= 0.0015
                    });
            if !load_shape_exact {
                errors.push(format!(
                    "profile {profile} recovery cost source lacks exact size-triggered group-commit batching"
                ));
            }

            for bound in E3_BOUNDS {
                let prefix = format!("bound_{bound}");
                if !value_true(row, &format!("{prefix}_bar_met")) {
                    errors.push(format!("profile {profile} bound {bound} bar failed"));
                }
                let commands = value_u64(row, &format!("{prefix}_commands_committed"), &mut errors);
                let objects = value_u64(row, &format!("{prefix}_objects_put"), &mut errors);
                let segments = value_u64(row, &format!("{prefix}_segments_sealed"), &mut errors);
                let puts = value_u64(row, &format!("{prefix}_store_put_requests"), &mut errors);
                let gets = value_u64(row, &format!("{prefix}_store_get_requests"), &mut errors);
                let lists = value_u64(row, &format!("{prefix}_store_list_requests"), &mut errors);
                let deletes =
                    value_u64(row, &format!("{prefix}_store_delete_requests"), &mut errors);
                let request_bytes =
                    value_u64(row, &format!("{prefix}_store_request_bytes"), &mut errors);
                let response_bytes =
                    value_u64(row, &format!("{prefix}_store_response_bytes"), &mut errors);
                let Some((
                    commands,
                    objects,
                    segments,
                    puts,
                    gets,
                    lists,
                    deletes,
                    request_bytes,
                    response_bytes,
                )) = commands
                    .zip(objects)
                    .zip(segments)
                    .zip(puts)
                    .zip(gets)
                    .zip(lists)
                    .zip(deletes)
                    .zip(request_bytes)
                    .zip(response_bytes)
                    .map(
                        |(
                            (
                                ((((((commands, objects), segments), puts), gets), lists), deletes),
                                request_bytes,
                            ),
                            response_bytes,
                        )| {
                            (
                                commands,
                                objects,
                                segments,
                                puts,
                                gets,
                                lists,
                                deletes,
                                request_bytes,
                                response_bytes,
                            )
                        },
                    )
                else {
                    continue;
                };
                if commands == 0 || segments == 0 || puts == 0 || request_bytes == 0 {
                    errors.push(format!(
                        "profile {profile} bound {bound} has empty measured counters"
                    ));
                    continue;
                }
                inputs.push(ReleaseCostInput {
                    backend_profile: profile.to_string(),
                    bound: bound.to_string(),
                    counts: ObjectLogCounts {
                        label: format!("live E3 {profile} {bound}"),
                        commands: commands as f64,
                        objects_put: objects as f64,
                        segments_sealed: segments as f64,
                        put_requests: puts as f64,
                        get_requests: gets as f64,
                        list_requests: lists as f64,
                        delete_requests: deletes as f64,
                        request_bytes: request_bytes as f64,
                        response_bytes: response_bytes as f64,
                        recovery_mode: if profile == "object_log_sqlite_projection" {
                            RecoveryMode::SnapshotTail
                        } else {
                            RecoveryMode::FullGenesis
                        },
                        recovery_commands: total.unwrap_or(0) as f64,
                        recovery_put_requests: recovery_puts.unwrap_or(0) as f64,
                        recovery_get_requests: recovery_gets.unwrap_or(0) as f64,
                        recovery_list_requests: recovery_lists.unwrap_or(0) as f64,
                        recovery_delete_requests: recovery_deletes.unwrap_or(0) as f64,
                        recovery_request_bytes: recovery_request_bytes.unwrap_or(0) as f64,
                        recovery_response_bytes: recovery_response_bytes.unwrap_or(0) as f64,
                    },
                    source_command: row.command.clone(),
                    source_environment: row.environment.clone(),
                    source_revision: source_revision.to_string(),
                });
            }
        }
        if inputs.len() != E3_PROFILES.len() * E3_BOUNDS.len() {
            errors.push(format!(
                "extracted {} release cost inputs; expected 8",
                inputs.len()
            ));
        }
        let revisions = inputs
            .iter()
            .map(|input| input.source_revision.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if revisions.len() != 1 {
            errors.push("release cost inputs do not share one exact source revision".into());
        }
        if errors.is_empty() {
            Ok(inputs)
        } else {
            Err(errors)
        }
    }

    /// Workload + retention/recovery assumptions, and the MEASURED `postgres_native` E0 throughput. Defaults
    /// from [`WorkloadAssumptions::tp002_high_volume_baseline`].
    #[derive(Debug, Clone, PartialEq)]
    pub struct WorkloadAssumptions {
        /// Compute billing window, hours. Default 730 (an always-on month): the queue's DB instance and the
        /// object-log node both run continuously to hold the resident backlog and serve live traffic — you do
        /// not tear the queue authority down between batches. [`CostBreakdown`] also reports the
        /// `processing_hours` it takes to push a billion commands through at the measured throughput, which
        /// confirms one always-on instance has ample headroom.
        pub billing_window_hours: f64,
        /// Logical bytes per durable command record (ADR-001 baseline: 1 KiB encoded record).
        pub bytes_per_command: f64,
        /// Commands per fully-processed item (push + claim + finalize ⇒ 3); used to fold the measured E0
        /// ingest and claim+finalize item rates into an end-to-end command throughput.
        pub commands_per_item: f64,
        /// Resident durable working set (items) the backend must retain. Default 10,000,000 — the E0/E3 shape.
        pub resident_items: f64,
        /// Index/tuple overhead multiplier on the relational store's resident bytes (heap + claim/priority
        /// indexes + idempotency). Object storage retains the projection snapshot without this DB overhead.
        pub pg_index_overhead: f64,
        /// Provisioned IOPS the `postgres_native` claim-index churn must reserve to stay off the IOPS floor
        /// (the MVCC-bloat finding shows the drain is read-IOPS-bound). Set to 0 to model free local disk.
        pub pg_provisioned_iops: f64,
        /// Durable-log recovery window, hours: how much committed log object storage retains *behind* the
        /// latest snapshot so a node can rebuild. Object-log storage cost is tied to THIS, not total history.
        pub recovery_window_hours: f64,
        /// How many full snapshot+tail recoveries happen per billing window (drives recovery GET volume).
        pub recoveries_per_window: f64,
        /// MEASURED E0 ingest throughput, items/s (`docs/perf/tp002-e0e1-postgres-release-10m.md`).
        pub pg_ingest_per_s: f64,
        /// MEASURED E0 claim+finalize (drain) throughput, items/s.
        pub pg_claim_finalize_per_s: f64,
        /// Whether normalization includes claim+finalize. Live E3 request density measures push commands,
        /// so release cost rows set this false and compare against postgres ingest throughput.
        pub pg_claim_finalize_in_scope: bool,
    }

    impl WorkloadAssumptions {
        /// The TP-002 high-volume baseline: an always-on month, 1 KiB records, 10M resident, a 24 h recovery
        /// window, and the MEASURED E0 throughputs (ingest 20,431/s, claim+finalize 6,145/s). The provisioned
        /// IOPS default (12,000) reserves headroom for the claim-index churn the E0 evidence documents.
        pub fn tp002_high_volume_baseline() -> Self {
            WorkloadAssumptions {
                billing_window_hours: HOURS_PER_MONTH,
                bytes_per_command: 1024.0,
                commands_per_item: 3.0,
                resident_items: 10_000_000.0,
                pg_index_overhead: 2.5,
                pg_provisioned_iops: 12_000.0,
                recovery_window_hours: 24.0,
                recoveries_per_window: 1.0,
                pg_ingest_per_s: 20_431.0,
                pg_claim_finalize_per_s: 6_145.0,
                pg_claim_finalize_in_scope: true,
            }
        }

        /// Release E3 request accounting is normalized to one billion durable push commands; it does not
        /// extrapolate push segment density across claim/finalize command types that were not measured.
        pub fn tp002_e3_push_baseline() -> Self {
            let mut workload = Self::tp002_high_volume_baseline();
            workload.commands_per_item = 1.0;
            workload.pg_claim_finalize_in_scope = false;
            workload
        }

        /// End-to-end command throughput (commands/s) folded from the measured per-item E0 rates: each item is
        /// one push (at the ingest rate) plus a claim+finalize pair (at the drain rate); the per-item wall time
        /// is the sum, and the command rate is `commands_per_item / per_item_seconds`.
        pub fn pg_command_throughput_per_s(&self) -> f64 {
            if !self.pg_claim_finalize_in_scope {
                return self.pg_ingest_per_s;
            }
            let per_item_seconds = 1.0 / self.pg_ingest_per_s + 1.0 / self.pg_claim_finalize_per_s;
            self.commands_per_item / per_item_seconds
        }
    }

    /// The itemized cost of ONE backend for a billion commands. Every line is a separate, inspectable term.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CostBreakdown {
        /// Object-log: PUT requests scaled to a billion commands. Postgres: 0.
        pub put_requests: f64,
        /// Object-log: cost of those PUTs. Postgres: 0.
        pub put_cost: f64,
        /// Object-log: recovery GET requests over the billing window. Postgres: 0.
        pub get_requests: f64,
        /// Object-log: cost of those recovery GETs. Postgres: 0.
        pub get_cost: f64,
        /// Object-log: LIST requests scaled to a billion commands. Postgres: 0.
        pub list_requests: f64,
        /// Object-log: cost of those LISTs. Postgres: 0.
        pub list_cost: f64,
        /// Object-log: DELETE requests scaled to a billion commands. Postgres: 0.
        pub delete_requests: f64,
        /// Object-log: cost of those DELETEs. Postgres: 0 (S3 Standard currently prices DELETE at zero).
        pub delete_cost: f64,
        /// Durable bytes retained (GB): object-log = snapshot + recovery-window log; postgres = resident heap
        /// + index overhead.
        pub storage_gb: f64,
        /// Cost of the retained storage over the billing window.
        pub storage_cost: f64,
        /// Provisioned IOPS reserved (postgres only).
        pub provisioned_iops: f64,
        /// Cost of the provisioned IOPS over the billing window (postgres only).
        pub iops_cost: f64,
        /// Compute node hours billed (the always-on window).
        pub compute_hours: f64,
        /// Hours to push a billion commands through at the measured throughput (utilization check; ≤ window).
        pub processing_hours: f64,
        /// Cost of the compute node over the billing window.
        pub compute_cost: f64,
        /// Sum of every line above — the backend's `$/billion-commands`.
        pub total: f64,
    }

    /// The structured comparison: each backend's `$/billion-commands` with full breakdown, the ratio, and which
    /// side wins under the supplied inputs.
    #[derive(Debug, Clone, PartialEq)]
    pub struct CostComparison {
        /// `object_log_sqlite_projection` total `$/billion-commands`.
        pub objectlog_per_billion: f64,
        /// `postgres_native` total `$/billion-commands`.
        pub postgres_per_billion: f64,
        /// `postgres_per_billion / objectlog_per_billion` (> 1 ⇒ object-log is cheaper, by this multiple).
        pub ratio: f64,
        /// `true` iff `objectlog_per_billion < postgres_per_billion` under these inputs (NOT hard-coded).
        pub objectlog_wins: bool,
        /// End-to-end postgres command throughput used for `processing_hours` (commands/s).
        pub pg_command_throughput_per_s: f64,
        /// Itemized object-log cost.
        pub objectlog: CostBreakdown,
        /// Itemized postgres cost.
        pub postgres: CostBreakdown,
    }

    /// Compute the `$/billion-commands` comparison. Pure: a deterministic function of `(counts, workload,
    /// prices)`, no IO.
    pub fn compute_comparison(
        counts: &ObjectLogCounts,
        w: &WorkloadAssumptions,
        p: &PriceInputs,
    ) -> CostComparison {
        let month_fraction = w.billing_window_hours / HOURS_PER_MONTH;

        // ----- object_log_sqlite_projection -----
        let has_measured_recovery = counts.recovery_commands > 0.0;
        let put_requests = counts.put_requests / counts.commands * BILLION
            + if has_measured_recovery {
                counts.recovery_put_requests * w.recoveries_per_window
            } else {
                0.0
            };
        let put_cost = put_requests / 1000.0 * p.s3_put_per_1k;

        // Durable storage: the projection snapshot (resident working set) + the committed log retained behind
        // it for the recovery window. The command rate at a billion-per-window sets how much log a window holds.
        let command_rate_per_hour = BILLION / w.billing_window_hours;
        let (snapshot_bytes, recovery_log_commands) = match counts.recovery_mode {
            RecoveryMode::SnapshotTail => (
                w.resident_items * w.bytes_per_command,
                command_rate_per_hour * w.recovery_window_hours,
            ),
            RecoveryMode::FullGenesis => (0.0, BILLION),
        };
        let recovery_log_bytes = recovery_log_commands * w.bytes_per_command;
        let ol_storage_gb = (snapshot_bytes + recovery_log_bytes) / BYTES_PER_GB;
        let ol_storage_cost = ol_storage_gb * p.s3_storage_per_gb_month * month_fraction;

        // The governed recovery probe rebuilds the fixed 10M resident shape. Its load uses batched push
        // commands solely to keep the live run tractable, so `recovery_commands` is provenance, not a valid
        // denominator for push-only billion-command normalization. Charge the exact measured 10M rebuild once
        // per configured recovery; extrapolating by batched load commands would invent request volume.
        let measured_get_requests = counts.get_requests / counts.commands * BILLION;
        let recovery_get_requests = if has_measured_recovery {
            counts.recovery_get_requests * w.recoveries_per_window
        } else {
            (recovery_log_commands / counts.commands_per_segment() + 1.0) * w.recoveries_per_window
        };
        let get_requests = measured_get_requests + recovery_get_requests;
        let get_cost = get_requests / 1000.0 * p.s3_get_per_1k;
        let list_requests = counts.list_requests / counts.commands * BILLION
            + if has_measured_recovery {
                counts.recovery_list_requests * w.recoveries_per_window
            } else {
                0.0
            };
        let list_cost = list_requests / 1000.0 * p.s3_put_per_1k;
        let delete_requests = counts.delete_requests / counts.commands * BILLION
            + if has_measured_recovery {
                counts.recovery_delete_requests * w.recoveries_per_window
            } else {
                0.0
            };
        let delete_cost = delete_requests / 1000.0 * p.s3_delete_per_1k;

        let ol_compute_hours = w.billing_window_hours;
        let processing_hours = BILLION / w.pg_command_throughput_per_s() / 3600.0;
        let ol_compute_cost = ol_compute_hours * p.objectlog_node_per_hour;

        let objectlog = CostBreakdown {
            put_requests,
            put_cost,
            get_requests,
            get_cost,
            list_requests,
            list_cost,
            delete_requests,
            delete_cost,
            storage_gb: ol_storage_gb,
            storage_cost: ol_storage_cost,
            provisioned_iops: 0.0,
            iops_cost: 0.0,
            compute_hours: ol_compute_hours,
            processing_hours,
            compute_cost: ol_compute_cost,
            total: put_cost
                + get_cost
                + list_cost
                + delete_cost
                + ol_storage_cost
                + ol_compute_cost,
        };

        // ----- postgres_native -----
        let pg_storage_gb =
            w.resident_items * w.bytes_per_command * w.pg_index_overhead / BYTES_PER_GB;
        let pg_storage_cost = pg_storage_gb * p.pg_storage_per_gb_month * month_fraction;
        let pg_iops_cost = w.pg_provisioned_iops * p.pg_iops_per_month_each * month_fraction;
        let pg_compute_cost = w.billing_window_hours * p.pg_instance_per_hour;

        let postgres = CostBreakdown {
            put_requests: 0.0,
            put_cost: 0.0,
            get_requests: 0.0,
            get_cost: 0.0,
            list_requests: 0.0,
            list_cost: 0.0,
            delete_requests: 0.0,
            delete_cost: 0.0,
            storage_gb: pg_storage_gb,
            storage_cost: pg_storage_cost,
            provisioned_iops: w.pg_provisioned_iops,
            iops_cost: pg_iops_cost,
            compute_hours: w.billing_window_hours,
            processing_hours,
            compute_cost: pg_compute_cost,
            total: pg_compute_cost + pg_storage_cost + pg_iops_cost,
        };

        let ratio = postgres.total / objectlog.total;
        CostComparison {
            objectlog_per_billion: objectlog.total,
            postgres_per_billion: postgres.total,
            ratio,
            objectlog_wins: objectlog.total < postgres.total,
            pg_command_throughput_per_s: w.pg_command_throughput_per_s(),
            objectlog,
            postgres,
        }
    }

    /// Build the TP-002 E3 **cost-model** ledger row from a computed comparison. The row is **smoke-tier**: it
    /// is a derived CALCULATION over the measured E3/E0 counts (cited prices, stated assumptions), NOT a fresh
    /// live measurement — so it is recorded and strict-validated for visibility but never counts as headline
    /// release evidence on its own (the live S3 E3 run carries the release-tier `E3`). It is traceable
    /// (`tp002_evidence_ids=["E3"]`) and carries the computed numbers + the inputs that produced them.
    pub fn build_cost_row(
        comparison: &CostComparison,
        counts: &ObjectLogCounts,
        w: &WorkloadAssumptions,
        p: &PriceInputs,
        command: &str,
    ) -> LedgerRow {
        let round2 = |x: f64| (x * 100.0).round() / 100.0;
        let values = BTreeMap::from([
            ("cost_model".to_string(), serde_json::json!(true)),
            (
                "objectlog_usd_per_billion_commands".to_string(),
                serde_json::json!(round2(comparison.objectlog_per_billion)),
            ),
            (
                "postgres_usd_per_billion_commands".to_string(),
                serde_json::json!(round2(comparison.postgres_per_billion)),
            ),
            (
                "postgres_over_objectlog_ratio".to_string(),
                serde_json::json!(round2(comparison.ratio)),
            ),
            (
                "objectlog_below_postgres".to_string(),
                serde_json::json!(comparison.objectlog_wins),
            ),
            (
                "objectlog_put_cost_usd".to_string(),
                serde_json::json!(round2(comparison.objectlog.put_cost)),
            ),
            (
                "objectlog_node_compute_usd".to_string(),
                serde_json::json!(round2(comparison.objectlog.compute_cost)),
            ),
            (
                "objectlog_storage_usd".to_string(),
                serde_json::json!(round2(comparison.objectlog.storage_cost)),
            ),
            (
                "postgres_compute_usd".to_string(),
                serde_json::json!(round2(comparison.postgres.compute_cost)),
            ),
            (
                "postgres_provisioned_iops_usd".to_string(),
                serde_json::json!(round2(comparison.postgres.iops_cost)),
            ),
            (
                "postgres_processing_hours_per_billion".to_string(),
                serde_json::json!(round2(comparison.postgres.processing_hours)),
            ),
            (
                "objectlog_counts_label".to_string(),
                serde_json::json!(counts.label),
            ),
            (
                "objects_per_command".to_string(),
                serde_json::json!(round2(counts.objects_put / counts.commands * 1000.0) / 1000.0),
            ),
            (
                "billing_window_hours".to_string(),
                serde_json::json!(w.billing_window_hours),
            ),
            (
                "recovery_window_hours".to_string(),
                serde_json::json!(w.recovery_window_hours),
            ),
            (
                "pg_provisioned_iops".to_string(),
                serde_json::json!(w.pg_provisioned_iops),
            ),
            (
                "price_source".to_string(),
                serde_json::json!(p.instance_source),
            ),
            (
                "iops_price_source".to_string(),
                serde_json::json!(p.iops_source),
            ),
        ]);

        LedgerRow {
            suite: "tp002_e3_cost_model".into(),
            command: command.into(),
            backend_profile: "object_log_sqlite_projection".into(),
            scale: "smoke".into(),
            seed: 0,
            environment: format!(
                "derived cost model (fireweed-cost-model): REAL E3 counts ({label}: {cmds} commands, \
                 {objs} objects, {segs} segments) scaled to 1e9 commands vs postgres_native at the measured \
                 E0 throughput ({tput:.0} commands/s); cited prices [{src}]; always-on {win}h window, \
                 {iops} provisioned IOPS",
                label = counts.label,
                cmds = counts.commands,
                objs = counts.objects_put,
                segs = counts.segments_sealed,
                tput = comparison.pg_command_throughput_per_s,
                src = p.instance_source,
                win = w.billing_window_hours,
                iops = w.pg_provisioned_iops,
            ),
            exit_status: 0,
            ac_ids: vec![],
            inv_ids: vec![],
            pass_bar:
                "E3 cost model: object_log_sqlite_projection $/billion-commands < postgres_native \
                 $/billion-commands at the documented high-volume baseline with cited prices"
                    .into(),
            evidence_tier: "smoke".into(),
            measurements: Measurements {
                tp002_evidence_ids: vec!["E3".into()],
                values,
            },
        }
    }

    /// Build one release-tier cost row for every governed E3 profile/bound input.
    pub fn build_release_cost_rows(
        inputs: &[ReleaseCostInput],
        w: &WorkloadAssumptions,
        p: &PriceInputs,
        command: &str,
    ) -> Result<Vec<LedgerRow>, Vec<String>> {
        if inputs.len() != 8 {
            return Err(vec![format!(
                "expected 8 release cost inputs, got {}",
                inputs.len()
            )]);
        }
        let comparisons = inputs
            .iter()
            .map(|input| compute_comparison(&input.counts, w, p))
            .collect::<Vec<_>>();
        let optimized = comparisons
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.objectlog_per_billion
                    .partial_cmp(&right.objectlog_per_billion)
                    .expect("finite cost")
            })
            .map(|(index, _)| index)
            .expect("non-empty inputs");
        if !comparisons[optimized].objectlog_wins {
            return Err(vec![format!(
                "cost-optimized point does not beat postgres_native: objectlog={:.2} postgres={:.2}",
                comparisons[optimized].objectlog_per_billion,
                comparisons[optimized].postgres_per_billion
            )]);
        }

        let round2 = |value: f64| (value * 100.0).round() / 100.0;
        Ok(inputs
            .iter()
            .zip(comparisons)
            .enumerate()
            .map(|(index, (input, comparison))| {
                let per_billion = |requests: f64| requests / input.counts.commands * BILLION;
                let put_requests = per_billion(input.counts.put_requests);
                let get_requests = per_billion(input.counts.get_requests);
                let list_requests = per_billion(input.counts.list_requests);
                let delete_requests = per_billion(input.counts.delete_requests);
                let request_bytes = per_billion(input.counts.request_bytes);
                let response_bytes = per_billion(input.counts.response_bytes);
                let total_request_bytes = request_bytes
                    + input.counts.recovery_request_bytes * w.recoveries_per_window;
                let total_response_bytes = response_bytes
                    + input.counts.recovery_response_bytes * w.recoveries_per_window;
                let total_requests = comparison.objectlog.put_requests
                    + comparison.objectlog.get_requests
                    + comparison.objectlog.list_requests
                    + comparison.objectlog.delete_requests;
                let total_bytes = total_request_bytes + total_response_bytes;
                let values = BTreeMap::from([
                    ("bars_met".into(), serde_json::json!(true)),
                    ("cost_model".into(), serde_json::json!(true)),
                    ("cost_bound".into(), serde_json::json!(input.bound)),
                    ("cost_optimized_point".into(), serde_json::json!(index == optimized)),
                    ("measured_counter_linked".into(), serde_json::json!(true)),
                    ("source_evidence_tier".into(), serde_json::json!("release")),
                    ("source_suite".into(), serde_json::json!("performance_object_log_e3_live_tests")),
                    ("source_revision".into(), serde_json::json!(input.source_revision)),
                    ("measured_commands".into(), serde_json::json!(input.counts.commands as u64)),
                    ("measured_objects_put".into(), serde_json::json!(input.counts.objects_put as u64)),
                    ("measured_segments_sealed".into(), serde_json::json!(input.counts.segments_sealed as u64)),
                    ("measured_store_put_requests".into(), serde_json::json!(input.counts.put_requests as u64)),
                    ("measured_store_get_requests".into(), serde_json::json!(input.counts.get_requests as u64)),
                    ("measured_store_list_requests".into(), serde_json::json!(input.counts.list_requests as u64)),
                    ("measured_store_delete_requests".into(), serde_json::json!(input.counts.delete_requests as u64)),
                    ("measured_store_request_bytes".into(), serde_json::json!(input.counts.request_bytes as u64)),
                    ("measured_store_response_bytes".into(), serde_json::json!(input.counts.response_bytes as u64)),
                    ("recovery_mode".into(), serde_json::json!(match input.counts.recovery_mode { RecoveryMode::SnapshotTail => "snapshot_tail", RecoveryMode::FullGenesis => "full_genesis" })),
                    ("measured_recovery_commands".into(), serde_json::json!(input.counts.recovery_commands as u64)),
                    ("measured_recovery_put_requests".into(), serde_json::json!(input.counts.recovery_put_requests as u64)),
                    ("measured_recovery_get_requests".into(), serde_json::json!(input.counts.recovery_get_requests as u64)),
                    ("measured_recovery_list_requests".into(), serde_json::json!(input.counts.recovery_list_requests as u64)),
                    ("measured_recovery_delete_requests".into(), serde_json::json!(input.counts.recovery_delete_requests as u64)),
                    ("measured_recovery_request_bytes".into(), serde_json::json!(input.counts.recovery_request_bytes as u64)),
                    ("measured_recovery_response_bytes".into(), serde_json::json!(input.counts.recovery_response_bytes as u64)),
                    ("steady_state_put_requests_per_billion".into(), serde_json::json!(round2(put_requests))),
                    ("put_requests_per_billion".into(), serde_json::json!(round2(comparison.objectlog.put_requests))),
                    ("steady_state_get_requests_per_billion".into(), serde_json::json!(round2(get_requests))),
                    ("steady_state_list_requests_per_billion".into(), serde_json::json!(round2(list_requests))),
                    ("steady_state_delete_requests_per_billion".into(), serde_json::json!(round2(delete_requests))),
                    ("steady_state_request_bytes_per_billion".into(), serde_json::json!(round2(request_bytes))),
                    ("steady_state_response_bytes_per_billion".into(), serde_json::json!(round2(response_bytes))),
                    ("request_bytes_per_billion".into(), serde_json::json!(round2(total_request_bytes))),
                    ("response_bytes_per_billion".into(), serde_json::json!(round2(total_response_bytes))),
                    ("requests_per_billion".into(), serde_json::json!(round2(total_requests))),
                    ("bytes_per_billion".into(), serde_json::json!(round2(total_bytes))),
                    ("get_requests_per_billion".into(), serde_json::json!(round2(comparison.objectlog.get_requests))),
                    ("list_requests_per_billion".into(), serde_json::json!(round2(comparison.objectlog.list_requests))),
                    ("delete_requests_per_billion".into(), serde_json::json!(round2(comparison.objectlog.delete_requests))),
                    ("put_usd_per_billion".into(), serde_json::json!(round2(comparison.objectlog.put_cost))),
                    ("get_usd_per_billion".into(), serde_json::json!(round2(comparison.objectlog.get_cost))),
                    ("list_usd_per_billion".into(), serde_json::json!(round2(comparison.objectlog.list_cost))),
                    ("delete_usd_per_billion".into(), serde_json::json!(round2(comparison.objectlog.delete_cost))),
                    ("objectlog_usd_per_billion_commands".into(), serde_json::json!(round2(comparison.objectlog_per_billion))),
                    ("usd_per_billion".into(), serde_json::json!(round2(comparison.objectlog_per_billion))),
                    ("postgres_usd_per_billion_commands".into(), serde_json::json!(round2(comparison.postgres_per_billion))),
                    ("objectlog_below_postgres".into(), serde_json::json!(comparison.objectlog_wins)),
                    ("price_source".into(), serde_json::json!(p.instance_source)),
                    ("iops_price_source".into(), serde_json::json!(p.iops_source)),
                    ("price_source_revision".into(), serde_json::json!(PRICE_SOURCE_REVISION)),
                ]);
                LedgerRow {
                    suite: "tp002_e3_release_cost_model".into(),
                    command: command.into(),
                    backend_profile: input.backend_profile.clone(),
                    scale: "release".into(),
                    seed: 0,
                    environment: format!(
                        "release cost calculation linked to live E3 bound {} [{}]; {}",
                        input.bound, input.source_command, input.source_environment
                    ),
                    exit_status: 0,
                    ac_ids: vec![],
                    inv_ids: vec![],
                    pass_bar: "measured request costs at this bound; optimized E3 point must beat documented postgres_native comparator".into(),
                    evidence_tier: "release".into(),
                    measurements: Measurements {
                        tp002_evidence_ids: vec!["E3".into()],
                        values,
                    },
                }
            })
            .collect())
    }

    /// Semantic validator for the release cost matrix.
    pub fn validate_release_cost_rows(rows: &[LedgerRow]) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut source_revisions = std::collections::BTreeSet::new();
        let mut computed = Vec::new();
        let prices = PriceInputs::adr_001_us_east_1();
        let workload = WorkloadAssumptions::tp002_e3_push_baseline();
        let number = |row: &LedgerRow, key: &str| {
            row.measurements.values.get(key).and_then(|value| {
                value
                    .as_f64()
                    .or_else(|| value.as_u64().map(|value| value as f64))
            })
        };
        for row in rows {
            let bound = row
                .measurements
                .values
                .get("cost_bound")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if row.scale != "release" || row.evidence_tier != "release" {
                errors.push(format!(
                    "{} {bound} cost row is not release-tier",
                    row.backend_profile
                ));
            }
            if !E3_PROFILES.contains(&row.backend_profile.as_str()) {
                errors.push(format!("unexpected profile {}", row.backend_profile));
            }
            if row.suite != "tp002_e3_release_cost_model"
                || row.exit_status != 0
                || row.measurements.tp002_evidence_ids != ["E3"]
            {
                errors.push(format!(
                    "{} {bound} has invalid cost-row identity/status/E3 linkage",
                    row.backend_profile
                ));
            }
            if !E3_BOUNDS.contains(&bound) {
                errors.push(format!(
                    "{} has unknown/missing bound {bound}",
                    row.backend_profile
                ));
            }
            if !seen.insert((row.backend_profile.clone(), bound.to_string())) {
                errors.push(format!(
                    "duplicate cost row {} {bound}",
                    row.backend_profile
                ));
            }
            if !value_true(row, "measured_counter_linked")
                || row
                    .measurements
                    .values
                    .get("measured_commands")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
                    == 0
                || row
                    .measurements
                    .values
                    .get("measured_store_put_requests")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
                    == 0
                || row
                    .measurements
                    .values
                    .get("measured_store_request_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
                    == 0
            {
                errors.push(format!(
                    "{} {bound} missing measured-counter linkage",
                    row.backend_profile
                ));
            }
            if !value_true(row, "bars_met")
                || row
                    .measurements
                    .values
                    .get("source_evidence_tier")
                    .and_then(serde_json::Value::as_str)
                    != Some("release")
                || row
                    .measurements
                    .values
                    .get("source_suite")
                    .and_then(serde_json::Value::as_str)
                    != Some("performance_object_log_e3_live_tests")
            {
                errors.push(format!(
                    "{} {bound} missing governed release source linkage",
                    row.backend_profile
                ));
            }
            let source_revision = row
                .measurements
                .values
                .get("source_revision")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            source_revisions.insert(source_revision.to_string());
            if source_revision.len() != 40
                || !source_revision.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                errors.push(format!(
                    "{} {bound} lacks exact source revision linkage",
                    row.backend_profile
                ));
            }
            if row
                .measurements
                .values
                .get("price_source_revision")
                .and_then(serde_json::Value::as_str)
                != Some(PRICE_SOURCE_REVISION)
            {
                errors.push(format!(
                    "{} {bound} has stale price provenance",
                    row.backend_profile
                ));
            }
            if row
                .measurements
                .values
                .get("price_source")
                .and_then(serde_json::Value::as_str)
                != Some(prices.instance_source)
                || row
                    .measurements
                    .values
                    .get("iops_price_source")
                    .and_then(serde_json::Value::as_str)
                    != Some(prices.iops_source)
            {
                errors.push(format!(
                    "{} {bound} price provenance text does not match the governed bundle",
                    row.backend_profile
                ));
            }

            let recovery_mode = match row
                .measurements
                .values
                .get("recovery_mode")
                .and_then(serde_json::Value::as_str)
            {
                Some("snapshot_tail") => Some(RecoveryMode::SnapshotTail),
                Some("full_genesis") => Some(RecoveryMode::FullGenesis),
                _ => None,
            };
            let expected_recovery_mode = if row.backend_profile == "object_log_sqlite_projection" {
                RecoveryMode::SnapshotTail
            } else {
                RecoveryMode::FullGenesis
            };
            if recovery_mode.is_some_and(|mode| mode != expected_recovery_mode) {
                errors.push(format!(
                    "{} {bound} recovery mode does not match profile",
                    row.backend_profile
                ));
            }
            let required = [
                "measured_commands",
                "measured_objects_put",
                "measured_segments_sealed",
                "measured_store_put_requests",
                "measured_store_get_requests",
                "measured_store_list_requests",
                "measured_store_delete_requests",
                "measured_store_request_bytes",
                "measured_store_response_bytes",
                "measured_recovery_commands",
                "measured_recovery_put_requests",
                "measured_recovery_get_requests",
                "measured_recovery_list_requests",
                "measured_recovery_delete_requests",
                "measured_recovery_request_bytes",
                "measured_recovery_response_bytes",
            ];
            let measured = required
                .iter()
                .map(|key| number(row, key))
                .collect::<Vec<_>>();
            if recovery_mode.is_none() || measured.iter().any(Option::is_none) {
                errors.push(format!(
                    "{} {bound} lacks inputs required to recompute cost",
                    row.backend_profile
                ));
                continue;
            }
            let counts = ObjectLogCounts {
                label: format!("recomputed {} {bound}", row.backend_profile),
                commands: measured[0].unwrap(),
                objects_put: measured[1].unwrap(),
                segments_sealed: measured[2].unwrap(),
                put_requests: measured[3].unwrap(),
                get_requests: measured[4].unwrap(),
                list_requests: measured[5].unwrap(),
                delete_requests: measured[6].unwrap(),
                request_bytes: measured[7].unwrap(),
                response_bytes: measured[8].unwrap(),
                recovery_mode: recovery_mode.unwrap(),
                recovery_commands: measured[9].unwrap(),
                recovery_put_requests: measured[10].unwrap(),
                recovery_get_requests: measured[11].unwrap(),
                recovery_list_requests: measured[12].unwrap(),
                recovery_delete_requests: measured[13].unwrap(),
                recovery_request_bytes: measured[14].unwrap(),
                recovery_response_bytes: measured[15].unwrap(),
            };
            let comparison = compute_comparison(&counts, &workload, &prices);
            let expected = [
                (
                    "put_requests_per_billion",
                    comparison.objectlog.put_requests,
                ),
                (
                    "get_requests_per_billion",
                    comparison.objectlog.get_requests,
                ),
                (
                    "list_requests_per_billion",
                    comparison.objectlog.list_requests,
                ),
                (
                    "delete_requests_per_billion",
                    comparison.objectlog.delete_requests,
                ),
                ("put_usd_per_billion", comparison.objectlog.put_cost),
                ("get_usd_per_billion", comparison.objectlog.get_cost),
                ("list_usd_per_billion", comparison.objectlog.list_cost),
                ("delete_usd_per_billion", comparison.objectlog.delete_cost),
                (
                    "objectlog_usd_per_billion_commands",
                    comparison.objectlog_per_billion,
                ),
                (
                    "postgres_usd_per_billion_commands",
                    comparison.postgres_per_billion,
                ),
            ];
            for (key, expected_value) in expected {
                if number(row, key).is_none_or(|actual| (actual - expected_value).abs() > 0.011) {
                    errors.push(format!(
                        "{} {bound} derived {key} does not recompute",
                        row.backend_profile
                    ));
                }
            }
            let per_billion = |value: f64| value / counts.commands * BILLION;
            let total_request_bytes = per_billion(counts.request_bytes)
                + counts.recovery_request_bytes * workload.recoveries_per_window;
            let total_response_bytes = per_billion(counts.response_bytes)
                + counts.recovery_response_bytes * workload.recoveries_per_window;
            for (key, expected_value) in [
                ("request_bytes_per_billion", total_request_bytes),
                ("response_bytes_per_billion", total_response_bytes),
                (
                    "requests_per_billion",
                    comparison.objectlog.put_requests
                        + comparison.objectlog.get_requests
                        + comparison.objectlog.list_requests
                        + comparison.objectlog.delete_requests,
                ),
                (
                    "bytes_per_billion",
                    total_request_bytes + total_response_bytes,
                ),
                ("usd_per_billion", comparison.objectlog_per_billion),
            ] {
                if number(row, key).is_none_or(|actual| (actual - expected_value).abs() > 0.011) {
                    errors.push(format!(
                        "{} {bound} derived {key} does not recompute",
                        row.backend_profile
                    ));
                }
            }
            if value_true(row, "objectlog_below_postgres") != comparison.objectlog_wins {
                errors.push(format!(
                    "{} {bound} comparator result does not recompute",
                    row.backend_profile
                ));
            }
            computed.push((
                row.backend_profile.clone(),
                bound.to_string(),
                comparison.objectlog_per_billion,
                comparison.objectlog_wins,
                value_true(row, "cost_optimized_point"),
            ));
        }
        for profile in E3_PROFILES {
            for bound in E3_BOUNDS {
                if !seen.contains(&(profile.to_string(), bound.to_string())) {
                    errors.push(format!("missing profile/bound {profile} {bound}"));
                }
            }
        }
        if source_revisions.len() != 1 {
            errors.push("release cost rows do not share one exact source revision".into());
        }
        if let Some(expected_optimized) = computed
            .iter()
            .min_by(|left, right| left.2.partial_cmp(&right.2).unwrap())
        {
            let marked = computed.iter().filter(|row| row.4).collect::<Vec<_>>();
            if marked.len() != 1
                || marked[0].0 != expected_optimized.0
                || marked[0].1 != expected_optimized.1
            {
                errors
                    .push("cost-optimized marker does not identify the recomputed minimum".into());
            }
            if !expected_optimized.3 {
                errors.push("recomputed cost-optimized point does not beat postgres_native".into());
            }
        } else {
            errors.push("no recomputable cost rows".into());
        }
        if rows.len() != 8 {
            errors.push(format!(
                "release cost matrix has {} rows; expected 8",
                rows.len()
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// FIXTURE 1 — the REAL E3 size-dominant measured counts + the cited ADR-001 prices at the documented
        /// high-volume baseline: object_log_sqlite_projection is below postgres_native, and the numbers land
        /// where the hand calculation says.
        #[test]
        fn real_e3_counts_objectlog_below_postgres() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let p = PriceInputs::adr_001_us_east_1();
            let c = compute_comparison(&counts, &w, &p);

            // The headline claim: object-log is below postgres at the high-volume baseline.
            assert!(
                c.objectlog_wins,
                "object_log should be below postgres: ol={:.2} pg={:.2}",
                c.objectlog_per_billion, c.postgres_per_billion
            );
            assert!(c.ratio > 3.0, "expected >3x, got {:.2}x", c.ratio);

            // PUTs: 68/2048 objects/command * 1e9 / 1000 * $0.005 ≈ $166.
            assert!(
                (c.objectlog.put_cost - 166.02).abs() < 1.0,
                "put_cost={:.2}",
                c.objectlog.put_cost
            );
            // Postgres is dominated by its always-on instance + provisioned IOPS, not compute-time.
            assert!(
                (c.postgres.compute_cost - 201.48).abs() < 1.0,
                "pg compute={:.2}",
                c.postgres.compute_cost
            );
            assert!(
                (c.postgres.iops_cost - 780.0).abs() < 1.0,
                "pg iops={:.2}",
                c.postgres.iops_cost
            );
            // The instance-hours utilization check: one always-on instance has ample headroom (a billion
            // commands take ~20 h of a 730 h month at the measured throughput).
            assert!(
                c.postgres.processing_hours < 25.0 && c.postgres.processing_hours > 15.0,
                "processing_hours={:.2}",
                c.postgres.processing_hours
            );
        }

        /// FIXTURE 2 — the calculator RESPONDS to inputs (it is not hard-wired to "object-log wins"):
        /// cranking the S3 PUT price crosses the result over to postgres, and PUT cost is monotonic in price.
        #[test]
        fn crossover_when_put_price_cranked() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let base = PriceInputs::adr_001_us_east_1();
            let baseline = compute_comparison(&counts, &w, &base);
            assert!(baseline.objectlog_wins);

            // Crank S3 PUT 10x: object-log now exceeds postgres ⇒ the win flips. Not hard-coded.
            let mut dear = base.clone();
            dear.s3_put_per_1k = base.s3_put_per_1k * 10.0;
            let crossed = compute_comparison(&counts, &w, &dear);
            assert!(
                !crossed.objectlog_wins,
                "10x PUT price should flip the result: ol={:.2} pg={:.2}",
                crossed.objectlog_per_billion, crossed.postgres_per_billion
            );
            // Monotonic: higher PUT price ⇒ strictly higher object-log total.
            assert!(crossed.objectlog_per_billion > baseline.objectlog_per_billion);
        }

        /// FIXTURE 3 — the real crossover the artifact reports: with the postgres IOPS floor removed (free
        /// local disk) AND the pessimistic small E3 segments, postgres wins; filling segments to a production
        /// byte target flips object-log back ahead even at zero postgres IOPS. Proves the win is earned by the
        /// modelled terms, not assumed.
        #[test]
        fn iops_floor_and_segment_fill_drive_the_crossover() {
            let p = PriceInputs::adr_001_us_east_1();
            let mut w_no_iops = WorkloadAssumptions::tp002_high_volume_baseline();
            w_no_iops.pg_provisioned_iops = 0.0;

            // Zero postgres IOPS + tiny latency-bound E3 segments ⇒ postgres is the cheaper side.
            let tiny = compute_comparison(&ObjectLogCounts::e3_size_dominant(), &w_no_iops, &p);
            assert!(
                !tiny.objectlog_wins,
                "no-IOPS postgres should beat tiny-segment object-log: ol={:.2} pg={:.2}",
                tiny.objectlog_per_billion, tiny.postgres_per_billion
            );

            // Fill segments to 16 MiB at 1 KiB/command (2 objects/segment) ⇒ object-log wins even at 0 IOPS.
            let filled =
                ObjectLogCounts::filled("16 MiB fill", 16.0 * 1024.0 * 1024.0, 1024.0, 2.0);
            let big = compute_comparison(&filled, &w_no_iops, &p);
            assert!(
                big.objectlog_wins,
                "filled segments should beat no-IOPS postgres: ol={:.2} pg={:.2}",
                big.objectlog_per_billion, big.postgres_per_billion
            );
        }

        /// The win survives even when the object-log node is priced IDENTICALLY to the postgres instance —
        /// i.e. the apples-to-apples win does not depend on cherry-picking a smaller node; the storage/I/O term
        /// carries it.
        #[test]
        fn win_survives_equal_node_pricing() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let mut p = PriceInputs::adr_001_us_east_1();
            p.objectlog_node_per_hour = p.pg_instance_per_hour; // same node both sides
            let c = compute_comparison(&counts, &w, &p);
            assert!(
                c.objectlog_wins,
                "win must survive equal node pricing: ol={:.2} pg={:.2}",
                c.objectlog_per_billion, c.postgres_per_billion
            );
        }

        /// The folded command throughput matches the hand calculation from the measured E0 item rates.
        #[test]
        fn command_throughput_folds_measured_e0_rates() {
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            // 3 / (1/20431 + 1/6145) ≈ 14,173 commands/s.
            let t = w.pg_command_throughput_per_s();
            assert!((t - 14_172.6).abs() < 5.0, "throughput={t:.1}");
        }

        #[test]
        fn e3_release_comparator_normalizes_only_the_measured_push_operation() {
            let w = WorkloadAssumptions::tp002_e3_push_baseline();
            assert_eq!(w.commands_per_item, 1.0);
            assert!(!w.pg_claim_finalize_in_scope);
            assert_eq!(w.pg_command_throughput_per_s(), w.pg_ingest_per_s);
        }

        #[test]
        fn granularity_model_selects_latency_or_size_from_real_inputs() {
            let prices = PriceInputs::adr_001_us_east_1();
            let base = GranularityAssumptions {
                label: "production defaults".into(),
                active_queue_count: 1.0,
                command_rate_per_s: 1_000.0,
                input_batch_commands: 1.0,
                encoded_command_bytes: 1_024.0,
                target_segment_bytes: 262_144.0,
                max_latency_ms: 20.0,
                starting_recovery_index_entries: 0,
                billing_window_hours: HOURS_PER_MONTH,
                recovery_window_hours: 24.0,
            };
            let latency = estimate_granularity(&base, &prices).unwrap();
            assert_eq!(latency.seal_trigger, "latency");
            assert_eq!(latency.commands_per_segment, 20.0);
            assert_eq!(latency.segment_bytes, 20_480.0);
            assert!((latency.fill_ratio - 0.078125).abs() < f64::EPSILON);

            let mut high_rate = base;
            high_rate.command_rate_per_s = 20_000.0;
            let size = estimate_granularity(&high_rate, &prices).unwrap();
            assert_eq!(size.seal_trigger, "size");
            assert_eq!(size.commands_per_segment, 256.0);
            assert_eq!(size.segment_bytes, 262_144.0);
            assert_eq!(size.fill_ratio, 1.0);
            assert!(size.put_usd_per_billion_commands < latency.put_usd_per_billion_commands);

            let mut batched = high_rate;
            batched.input_batch_commands = 1_000.0;
            let batched = estimate_granularity(&batched, &prices).unwrap();
            assert_eq!(batched.seal_trigger, "size");
            assert_eq!(batched.commands_per_segment, 1_000.0);
            assert_eq!(batched.segment_bytes, 1_024_000.0);
            assert!(
                batched.fill_ratio > 1.0,
                "whole input batch may overshoot target"
            );
        }

        #[test]
        fn granularity_model_exposes_low_rate_and_oversized_command_costs() {
            let prices = PriceInputs::adr_001_us_east_1();
            let low_rate = GranularityAssumptions {
                label: "low rate".into(),
                active_queue_count: 1.0,
                command_rate_per_s: 10.0,
                input_batch_commands: 1.0,
                encoded_command_bytes: 1_024.0,
                target_segment_bytes: 262_144.0,
                max_latency_ms: 20.0,
                starting_recovery_index_entries: 0,
                billing_window_hours: HOURS_PER_MONTH,
                recovery_window_hours: 24.0,
            };
            let low = estimate_granularity(&low_rate, &prices).unwrap();
            assert_eq!(low.commands_per_segment, 1.0);
            assert_eq!(low.seal_trigger, "latency");
            assert!(low.put_usd_per_billion_commands > 35_000.0);
            assert_eq!(low.ending_recovery_index_height, 4);

            let mut fleet = low_rate.clone();
            fleet.active_queue_count = 1_000.0;
            let fleet = estimate_granularity(&fleet, &prices).unwrap();
            assert_eq!(fleet.commands_per_segment, low.commands_per_segment);
            assert_eq!(
                fleet.put_requests_per_billing_window,
                low.put_requests_per_billing_window * 1_000.0
            );

            let mut oversized = low_rate;
            oversized.encoded_command_bytes = 524_288.0;
            let large = estimate_granularity(&oversized, &prices).unwrap();
            assert_eq!(large.commands_per_segment, 1.0);
            assert_eq!(large.seal_trigger, "size");
            assert_eq!(large.fill_ratio, 2.0);
        }

        #[test]
        fn granularity_model_rejects_nonpositive_or_nonfinite_inputs() {
            let prices = PriceInputs::adr_001_us_east_1();
            let mut assumptions = GranularityAssumptions {
                label: "invalid".into(),
                active_queue_count: 1.0,
                command_rate_per_s: 0.0,
                input_batch_commands: 1.0,
                encoded_command_bytes: 1_024.0,
                target_segment_bytes: 262_144.0,
                max_latency_ms: 20.0,
                starting_recovery_index_entries: 0,
                billing_window_hours: HOURS_PER_MONTH,
                recovery_window_hours: 24.0,
            };
            assert!(estimate_granularity(&assumptions, &prices).is_err());
            assumptions.command_rate_per_s = f64::INFINITY;
            assert!(estimate_granularity(&assumptions, &prices).is_err());
            assumptions.command_rate_per_s = 1.0;
            assumptions.active_queue_count = 0.0;
            assert!(estimate_granularity(&assumptions, &prices).is_err());
        }

        #[test]
        fn granularity_model_integrates_recovery_index_height_transitions() {
            assert_eq!(recovery_index_put_requests(0, 1).unwrap(), (4, 0));
            assert_eq!(recovery_index_put_requests(0, 64).unwrap(), (319, 0));
            assert_eq!(recovery_index_put_requests(0, 65).unwrap(), (324, 1));
            assert_eq!(recovery_index_put_requests(262_144, 1).unwrap(), (7, 3));
            assert!(recovery_index_put_requests(u64::MAX, 1).is_err());
        }

        #[test]
        fn measured_10m_recovery_requests_are_charged_once_not_scaled_by_batch_commands() {
            let mut counts = ObjectLogCounts::e3_size_dominant();
            counts.get_requests = 100.0;
            counts.list_requests = 50.0;
            counts.recovery_commands = 10_000.0;
            counts.recovery_put_requests = 4.0;
            counts.recovery_get_requests = 20.0;
            counts.recovery_list_requests = 3.0;
            let w = WorkloadAssumptions::tp002_e3_push_baseline();
            let comparison = compute_comparison(&counts, &w, &PriceInputs::adr_001_us_east_1());
            let steady_puts = counts.put_requests / counts.commands * BILLION;
            let steady_gets = counts.get_requests / counts.commands * BILLION;
            let steady_lists = counts.list_requests / counts.commands * BILLION;
            assert_eq!(comparison.objectlog.put_requests - steady_puts, 4.0);
            assert_eq!(comparison.objectlog.get_requests - steady_gets, 20.0);
            assert_eq!(comparison.objectlog.list_requests - steady_lists, 3.0);
        }

        /// The smoke-tier cost row is traceable and strict-valid, and never masquerades as release evidence.
        #[test]
        fn cost_row_is_smoke_tier_and_traceable() {
            let counts = ObjectLogCounts::e3_size_dominant();
            let w = WorkloadAssumptions::tp002_high_volume_baseline();
            let p = PriceInputs::adr_001_us_east_1();
            let c = compute_comparison(&counts, &w, &p);
            let row = build_cost_row(&c, &counts, &w, &p, "fireweed-cost-model");
            assert_eq!(row.evidence_tier, "smoke");
            assert_eq!(row.measurements.tp002_evidence_ids, vec!["E3".to_string()]);
            assert!(super::super::strict_row_errors(&row).is_empty());
        }

        fn synthetic_release_source(profile: &str) -> LedgerRow {
            let sqlite = profile == "object_log_sqlite_projection";
            let mut values = BTreeMap::from([
                ("bars_met".into(), serde_json::json!(true)),
                ("portable_gate".into(), serde_json::json!(true)),
                ("quiet_host_required".into(), serde_json::json!(false)),
                ("host_speed_gate".into(), serde_json::json!(false)),
                (
                    "storage_topology_id".into(),
                    serde_json::json!("synthetic-s3-topology"),
                ),
                (
                    "storage_topology_description".into(),
                    serde_json::json!("synthetic provider-neutral S3 topology"),
                ),
                (
                    "storage_durability_claim".into(),
                    serde_json::json!("excluded"),
                ),
                (
                    "storage_authority_mode".into(),
                    serde_json::json!("native-create-only"),
                ),
                (
                    "source_revision".into(),
                    serde_json::json!("1111111111111111111111111111111111111111"),
                ),
                ("recovery_bar_met".into(), serde_json::json!(true)),
                ("recovery_resident".into(), serde_json::json!(10_000_000)),
                ("recovery_snapshot_used".into(), serde_json::json!(sqlite)),
                (
                    "recovery_start_seq".into(),
                    serde_json::json!(if sqlite { 99 } else { 0 }),
                ),
                (
                    "recovery_tail_replayed".into(),
                    serde_json::json!(if sqlite { 1 } else { 100 }),
                ),
                ("recovery_total_commands".into(), serde_json::json!(100)),
                ("recovery_command_count".into(), serde_json::json!(100)),
                (
                    "recovery_load_segments_sealed".into(),
                    serde_json::json!(15),
                ),
                (
                    "recovery_load_size_triggered_seals".into(),
                    serde_json::json!(14),
                ),
                (
                    "recovery_load_latency_triggered_seals".into(),
                    serde_json::json!(1),
                ),
                ("recovery_load_forced_seals".into(), serde_json::json!(0)),
                ("recovery_load_rollover_seals".into(), serde_json::json!(0)),
                (
                    "recovery_load_group_commit_batch_sum".into(),
                    serde_json::json!(if sqlite { 99 } else { 100 }),
                ),
                (
                    "recovery_load_command_count".into(),
                    serde_json::json!(if sqlite { 99 } else { 100 }),
                ),
                (
                    "recovery_load_segment_bytes".into(),
                    serde_json::json!(1_000),
                ),
                (
                    "recovery_load_mean_commands_per_segment".into(),
                    serde_json::json!(if sqlite { 6.6 } else { 6.667 }),
                ),
                (
                    "recovery_load_max_commands_per_segment".into(),
                    serde_json::json!(10),
                ),
                (
                    "recovery_replay_progress_samples".into(),
                    serde_json::json!(if sqlite { vec![99, 100] } else { vec![0, 100] }),
                ),
                ("recovery_store_put_requests".into(), serde_json::json!(1)),
                ("recovery_store_get_requests".into(), serde_json::json!(10)),
                ("recovery_store_list_requests".into(), serde_json::json!(2)),
                (
                    "recovery_store_delete_requests".into(),
                    serde_json::json!(0),
                ),
                (
                    "recovery_store_request_bytes".into(),
                    serde_json::json!(100),
                ),
                (
                    "recovery_store_response_bytes".into(),
                    serde_json::json!(10_000),
                ),
            ]);
            for bound in E3_BOUNDS {
                let prefix = format!("bound_{bound}");
                values.insert(format!("{prefix}_bar_met"), serde_json::json!(true));
                values.insert(
                    format!("{prefix}_commands_committed"),
                    serde_json::json!(100_000),
                );
                values.insert(format!("{prefix}_objects_put"), serde_json::json!(500));
                values.insert(format!("{prefix}_segments_sealed"), serde_json::json!(250));
                values.insert(
                    format!("{prefix}_store_put_requests"),
                    serde_json::json!(500),
                );
                values.insert(
                    format!("{prefix}_store_get_requests"),
                    serde_json::json!(10),
                );
                values.insert(
                    format!("{prefix}_store_list_requests"),
                    serde_json::json!(5),
                );
                values.insert(
                    format!("{prefix}_store_delete_requests"),
                    serde_json::json!(0),
                );
                values.insert(
                    format!("{prefix}_store_request_bytes"),
                    serde_json::json!(100_000),
                );
                values.insert(
                    format!("{prefix}_store_response_bytes"),
                    serde_json::json!(10_000),
                );
            }
            LedgerRow {
                suite: "performance_object_log_e3_live_tests".into(),
                command: "live-e3".into(),
                backend_profile: profile.into(),
                scale: "release".into(),
                seed: 0,
                environment: "live provider-neutral S3 test fixture".into(),
                exit_status: 0,
                ac_ids: vec![],
                inv_ids: vec![],
                pass_bar: "all E3 bars".into(),
                evidence_tier: "release".into(),
                measurements: Measurements {
                    tp002_evidence_ids: vec!["E3".into()],
                    values,
                },
            }
        }

        fn synthetic_release_cost_rows() -> Vec<LedgerRow> {
            let sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            let inputs = release_cost_inputs(&sources).unwrap();
            build_release_cost_rows(
                &inputs,
                &WorkloadAssumptions::tp002_e3_push_baseline(),
                &PriceInputs::adr_001_us_east_1(),
                "fireweed-cost-model",
            )
            .unwrap()
        }

        #[test]
        fn release_cost_validator_accepts_complete_measured_matrix() {
            validate_release_cost_rows(&synthetic_release_cost_rows()).unwrap();
        }

        #[test]
        fn release_cost_validator_rejects_smoke_tier_cost() {
            let mut rows = synthetic_release_cost_rows();
            rows[0].evidence_tier = "smoke".into();
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("not release-tier"))
            );
        }

        #[test]
        fn release_cost_inputs_reject_quiet_host_and_absolute_speed_gates() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            sources[0].environment = "deferred until a quiet host is available".into();
            sources[1].pass_bar = "throughput >= 2777.78 items/s floor".into();
            let errors = release_cost_inputs(&sources).unwrap_err();
            assert_eq!(
                errors
                    .iter()
                    .filter(|error| error.contains("quiet-host or absolute host-speed gate"))
                    .count(),
                2
            );
        }

        #[test]
        fn release_cost_inputs_require_explicit_portable_gate_marker() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            sources[0].measurements.values.remove("portable_gate");
            sources[1]
                .measurements
                .values
                .insert("host_speed_gate".into(), serde_json::json!(true));
            let errors = release_cost_inputs(&sources).unwrap_err();
            assert_eq!(
                errors
                    .iter()
                    .filter(|error| error.contains("quiet-host or absolute host-speed gate"))
                    .count(),
                2
            );
        }

        #[test]
        fn release_cost_validator_rejects_missing_measured_counter_linkage() {
            let mut rows = synthetic_release_cost_rows();
            rows[0]
                .measurements
                .values
                .insert("measured_counter_linked".into(), serde_json::json!(false));
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("missing measured-counter linkage"))
            );
            rows[1]
                .measurements
                .values
                .remove("measured_recovery_put_requests");
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("lacks inputs required to recompute cost"))
            );
        }

        #[test]
        fn release_cost_validator_rejects_tampered_aggregate_density() {
            let mut rows = synthetic_release_cost_rows();
            rows[0]
                .measurements
                .values
                .insert("requests_per_billion".into(), serde_json::json!(1));
            rows[1].measurements.values.remove("bytes_per_billion");
            rows[2]
                .measurements
                .values
                .insert("usd_per_billion".into(), serde_json::json!(0));
            let errors = validate_release_cost_rows(&rows).unwrap_err();
            for key in [
                "requests_per_billion",
                "bytes_per_billion",
                "usd_per_billion",
            ] {
                assert!(errors.iter().any(|error| error.contains(key)), "{errors:?}");
            }
        }

        #[test]
        fn release_cost_validator_rejects_missing_profile() {
            let mut rows = synthetic_release_cost_rows();
            rows.retain(|row| row.backend_profile != "object_log_inmemory_projection");
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("missing profile/bound object_log_inmemory_projection"))
            );
        }

        #[test]
        fn release_cost_validator_rejects_stale_price_provenance() {
            let mut rows = synthetic_release_cost_rows();
            rows[0]
                .measurements
                .values
                .insert("price_source_revision".into(), serde_json::json!("stale"));
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("stale price provenance"))
            );
        }

        #[test]
        fn release_cost_validator_rejects_wrong_recovery_mode_or_source_identity() {
            let mut rows = synthetic_release_cost_rows();
            let sqlite = rows
                .iter_mut()
                .find(|row| row.backend_profile == "object_log_sqlite_projection")
                .unwrap();
            sqlite
                .measurements
                .values
                .insert("recovery_mode".into(), serde_json::json!("full_genesis"));
            sqlite
                .measurements
                .values
                .insert("source_suite".into(), serde_json::json!("untrusted"));
            sqlite
                .measurements
                .values
                .insert("source_revision".into(), serde_json::json!("dirty"));
            let errors = validate_release_cost_rows(&rows).unwrap_err();
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("recovery mode does not match profile"))
            );
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("missing governed release source linkage"))
            );
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("lacks exact source revision linkage"))
            );
        }

        #[test]
        fn release_cost_validator_rejects_mixed_source_revisions() {
            let mut rows = synthetic_release_cost_rows();
            rows[0].measurements.values.insert(
                "source_revision".into(),
                serde_json::json!("2222222222222222222222222222222222222222"),
            );
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("do not share one exact source revision"))
            );
        }

        #[test]
        fn release_cost_inputs_reject_failed_recovery_bar() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            sources[0]
                .measurements
                .values
                .insert("recovery_bar_met".into(), serde_json::json!(false));
            assert!(
                release_cost_inputs(&sources)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("recovery bar failed"))
            );
        }

        #[test]
        fn release_cost_inputs_reject_inexact_recovery_range_or_progress_endpoint() {
            let sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();

            let mut inexact_range = sources.clone();
            let sqlite = inexact_range
                .iter_mut()
                .find(|row| row.backend_profile == "object_log_sqlite_projection")
                .unwrap();
            sqlite
                .measurements
                .values
                .insert("recovery_total_commands".into(), serde_json::json!(102));
            assert!(
                release_cost_inputs(&inexact_range).unwrap_err().iter().any(
                    |error| error.contains("exact command range and replay progress endpoints")
                )
            );

            let mut stale_progress = sources;
            let sqlite = stale_progress
                .iter_mut()
                .find(|row| row.backend_profile == "object_log_sqlite_projection")
                .unwrap();
            sqlite.measurements.values.insert(
                "recovery_replay_progress_samples".into(),
                serde_json::json!([99, 99]),
            );
            assert!(
                release_cost_inputs(&stale_progress)
                    .unwrap_err()
                    .iter()
                    .any(
                        |error| error.contains("exact command range and replay progress endpoints")
                    )
            );
        }

        #[test]
        fn release_cost_inputs_reject_missing_tampered_or_zero_load_batch_measurements() {
            let sources = || {
                E3_PROFILES
                    .iter()
                    .map(|profile| synthetic_release_source(profile))
                    .collect::<Vec<_>>()
            };

            let mut missing = sources();
            missing[0]
                .measurements
                .values
                .remove("recovery_load_mean_commands_per_segment");
            assert!(
                release_cost_inputs(&missing)
                    .unwrap_err()
                    .iter()
                    .any(|error| {
                        error.contains(
                            "missing finite measured recovery_load_mean_commands_per_segment",
                        )
                    })
            );

            let mut zero = sources();
            zero[0]
                .measurements
                .values
                .insert("recovery_load_segment_bytes".into(), serde_json::json!(0));
            assert!(release_cost_inputs(&zero).unwrap_err().iter().any(|error| {
                error.contains("lacks exact size-triggered group-commit batching")
            }));

            let mut tampered = sources();
            tampered[0].measurements.values.insert(
                "recovery_load_mean_commands_per_segment".into(),
                serde_json::json!(99.0),
            );
            tampered[1].measurements.values.insert(
                "recovery_load_max_commands_per_segment".into(),
                serde_json::json!(1),
            );
            let errors = release_cost_inputs(&tampered).unwrap_err();
            assert_eq!(
                errors
                    .iter()
                    .filter(
                        |error| error.contains("lacks exact size-triggered group-commit batching")
                    )
                    .count(),
                2
            );
        }

        #[test]
        fn release_cost_inputs_reject_zero_sqlite_command_count_without_panicking() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            let sqlite = sources
                .iter_mut()
                .find(|row| row.backend_profile == "object_log_sqlite_projection")
                .unwrap();
            sqlite
                .measurements
                .values
                .insert("recovery_command_count".into(), serde_json::json!(0));
            sqlite
                .measurements
                .values
                .insert("recovery_load_command_count".into(), serde_json::json!(0));
            let errors = release_cost_inputs(&sources).unwrap_err();
            assert!(errors.iter().any(|error| error
                .contains("lacks exact size-triggered group-commit batching")));
        }

        #[test]
        fn release_cost_inputs_require_measured_recovery_puts() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            sources[0]
                .measurements
                .values
                .remove("recovery_store_put_requests");
            assert!(
                release_cost_inputs(&sources)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("missing measured recovery_store_put_requests"))
            );
        }

        #[test]
        fn release_cost_inputs_reject_duplicate_or_invalid_source_rows() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            sources.push(sources[0].clone());
            assert!(
                release_cost_inputs(&sources)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("expected exactly one"))
            );
            sources.pop();
            sources[0].suite = "wrong-suite".into();
            assert!(
                release_cost_inputs(&sources)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("invalid source identity"))
            );
            sources[0].suite = "performance_object_log_e3_live_tests".into();
            sources[0]
                .measurements
                .values
                .insert("source_revision".into(), serde_json::json!("dirty"));
            assert!(
                release_cost_inputs(&sources)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("lacks an exact committed source revision"))
            );
        }

        #[test]
        fn release_cost_inputs_reject_undeclared_or_overclaimed_storage_topology() {
            let mut sources = E3_PROFILES
                .iter()
                .map(|profile| synthetic_release_source(profile))
                .collect::<Vec<_>>();
            sources[0].measurements.values.insert(
                "storage_topology_id".into(),
                serde_json::json!("invalid topology id"),
            );
            sources[1].measurements.values.insert(
                "storage_durability_claim".into(),
                serde_json::json!("host-restart-proven"),
            );
            let errors = release_cost_inputs(&sources).unwrap_err();
            assert_eq!(
                errors
                    .iter()
                    .filter(|e| e.contains("lacks a declared provider-neutral S3 topology"))
                    .count(),
                2
            );
        }

        #[test]
        fn release_cost_validator_rejects_tampered_derived_cost() {
            let mut rows = synthetic_release_cost_rows();
            rows[0]
                .measurements
                .values
                .insert("get_usd_per_billion".into(), serde_json::json!(999.0));
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("does not recompute"))
            );
        }

        #[test]
        fn release_cost_validator_rejects_wrong_optimized_marker() {
            let mut rows = synthetic_release_cost_rows();
            let actual = rows
                .iter()
                .position(|row| value_true(row, "cost_optimized_point"))
                .unwrap();
            let wrong = (actual + 1) % rows.len();
            rows[actual]
                .measurements
                .values
                .insert("cost_optimized_point".into(), serde_json::json!(false));
            rows[wrong]
                .measurements
                .values
                .insert("cost_optimized_point".into(), serde_json::json!(true));
            assert!(
                validate_release_cost_rows(&rows)
                    .unwrap_err()
                    .iter()
                    .any(|e| e.contains("recomputed minimum"))
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static LEDGER_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn run_owned_temp(label: &str) -> RunOwned {
        let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let run_root = std::env::temp_dir().join(format!(
            "fireweed-release-lib-{label}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&run_root);
        fs::create_dir_all(&run_root).unwrap();
        RunOwned::new(repository_root, &run_root, format!("{label}.jsonl")).unwrap()
    }

    fn row(suite: &str, exit: i32, evidence: &[&str]) -> LedgerRow {
        LedgerRow {
            suite: suite.into(),
            command: format!("cargo test {suite}"),
            backend_profile: "memory".into(),
            scale: "smoke".into(),
            seed: 7,
            environment: "in-process".into(),
            exit_status: exit,
            ac_ids: vec!["AC-E2E-1".into()],
            inv_ids: vec!["INV-1".into()],
            pass_bar: "floor held".into(),
            evidence_tier: "release".into(),
            measurements: Measurements {
                tp002_evidence_ids: evidence.iter().map(|s| s.to_string()).collect(),
                values: BTreeMap::from([("items_per_sec".into(), serde_json::json!(123456))]),
            },
        }
    }

    #[test]
    fn smoke_tier_evidence_does_not_count_toward_the_headline() {
        let path = run_owned_temp("tier");
        // A release E2 row and a SMOKE E3 row.
        append_row(&path, &row("release_e2", 0, &["E2"])).unwrap();
        let mut smoke = row("smoke_e3", 0, &["E3"]);
        smoke.evidence_tier = "smoke".into();
        append_row(&path, &smoke).unwrap();

        let s = verify_ledger(path.path(), true).unwrap();
        // Only the release E2 counts as headline evidence; the smoke E3 is tracked separately.
        assert!(s.evidence_ids.contains("E2") && !s.evidence_ids.contains("E3"));
        assert!(s.smoke_evidence_ids.contains("E3"));
        // A gate requiring E3 is NOT satisfied by the smoke row.
        assert_eq!(
            missing_evidence(&s, &["E3".to_string()]),
            vec!["E3".to_string()]
        );
        // A legacy row that OMITS evidence_tier deserializes as release (back-compat).
        let legacy = r#"{"suite":"s","command":"c","backend_profile":"memory","scale":"release","seed":1,"environment":"ci","exit_status":0,"pass_bar":"p","measurements":{"tp002_evidence_ids":["E0"]}}"#;
        let parsed: LedgerRow = serde_json::from_str(legacy).unwrap();
        assert_eq!(parsed.evidence_tier, "release");
        let _ = fs::remove_dir_all(path.run_root());
    }

    #[test]
    fn row_round_trips_jsonl() {
        let r = row("s", 0, &["E2"]);
        let parsed: LedgerRow = serde_json::from_str(&r.to_jsonl()).unwrap();
        assert_eq!(r, parsed);
        // The flattened measurement value survives the round-trip.
        assert_eq!(
            parsed.measurements.values["items_per_sec"],
            serde_json::json!(123456)
        );
    }

    #[test]
    fn strict_rejects_failed_and_untraceable_rows() {
        // A well-formed, traceable, exit-0 row has no strict errors.
        assert!(strict_row_errors(&row("ok", 0, &["E0"])).is_empty());
        // exit_status != 0.
        assert!(
            strict_row_errors(&row("bad", 1, &["E0"]))
                .iter()
                .any(|e| e.contains("exit_status"))
        );
        // no ac_ids and no evidence ids.
        let mut untraceable = row("u", 0, &[]);
        untraceable.ac_ids.clear();
        assert!(
            strict_row_errors(&untraceable)
                .iter()
                .any(|e| e.contains("untraceable"))
        );
    }

    #[test]
    fn ledger_path_is_run_owned_and_rejects_tracked_evidence() {
        let _environment_guard = LEDGER_ENV_LOCK.lock().unwrap();
        let previous = std::env::var_os("FIREWEED_LEDGER_DIR");
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let external =
            std::env::temp_dir().join(format!("fireweed-ledger-override-{}", std::process::id()));
        let _ = fs::remove_dir_all(&external);
        fs::create_dir_all(&external).unwrap();

        // SAFETY: this module serializes access to the process environment variable.
        unsafe { std::env::set_var("FIREWEED_LEDGER_DIR", &external) };
        let explicit = ledger_path(manifest_dir, "suite_a").unwrap();
        assert_eq!(explicit.path(), external.join("suite_a.jsonl"));

        unsafe { std::env::set_var("FIREWEED_LEDGER_DIR", "docs/perf/evidence") };
        assert!(matches!(
            ledger_path(manifest_dir, "suite_a"),
            Err(EvidenceIoError::TrackedEvidence(_))
        ));

        unsafe { std::env::remove_var("FIREWEED_LEDGER_DIR") };
        let default = ledger_path(manifest_dir, "suite_a").unwrap();
        assert!(default.path().starts_with(std::env::temp_dir()));
        assert!(
            !default.path().starts_with(
                Path::new(manifest_dir)
                    .join("../..")
                    .canonicalize()
                    .unwrap()
            )
        );

        match previous {
            Some(value) => unsafe { std::env::set_var("FIREWEED_LEDGER_DIR", value) },
            None => unsafe { std::env::remove_var("FIREWEED_LEDGER_DIR") },
        }

        let _ = fs::remove_dir_all(external);
        let _ = fs::remove_dir_all(default.run_root());
    }

    #[test]
    fn missing_evidence_reports_the_gap() {
        let mut s = LedgerSummary::default();
        s.evidence_ids.extend(["E2".to_string(), "E3".to_string()]);
        let missing = missing_evidence(&s, &["E0", "E1", "E2", "E3"].map(String::from));
        assert_eq!(missing, vec!["E0".to_string(), "E1".to_string()]);
    }
}
