//! Strict semantic validation for TP-002 E2 live owner-failover evidence.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Optional SP-06 post-fence/pre-serve object-store profile. Queue identity is deliberately absent: the
/// dedicated SP-04 recorder is isolated around one handoff arm instead of adding tenant/queue metric labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffObjectStoreProfile {
    pub samples: u64,
    pub queue_items: u64,
    pub scripted_request_latency_ms: u64,
    pub unapplied_tail_per_handoff: bool,
    pub physical_requests: u64,
    pub p95_modeled_handoff_latency_ms: u64,
    pub p95_perfect_cache_latency_ms: u64,
    pub immutable_gets: u64,
    pub repeated_immutable_gets: u64,
    pub avoidable_immutable_gets: u64,
    pub manifest_candidate_gets: u64,
    pub repeated_manifest_candidate_gets: u64,
    pub segment_gets: u64,
    pub immutable_response_bytes: u64,
    pub tail_commands_replayed: u64,
    pub first_local_read_requests: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailoverEvidence {
    pub schema_version: u32,
    pub suite: String,
    pub command: String,
    pub evidence_id: String,
    pub evidence_tier: String,
    pub scale: String,
    pub backend_profile: String,
    pub bars_met: bool,
    pub replicas: u32,
    pub image: String,
    pub image_id: String,
    pub source_revision: String,
    pub chart_revision: String,
    pub postgres_image: String,
    pub minio_image: String,
    pub old_owner_id: String,
    pub new_owner_id: String,
    pub old_epoch: u64,
    pub new_epoch: u64,
    pub stale_append_rejected_before_mutation: bool,
    pub snapshot_tail_recovered: bool,
    pub visible_items_before: u64,
    pub visible_items_after: u64,
    pub lost_work: u64,
    pub double_leases: u64,
    pub corrupt_writes: u64,
    pub moved_count: u64,
    pub retry_count: u64,
    pub retry_succeeded: bool,
    pub moved_endpoint: String,
    pub topology: String,
    pub hardware: String,
    pub seed: u64,
    pub duration_ms: u64,
    pub fault_schedule: String,
    pub exclusions: String,
    /// `None` for historical/live failover runs that predate the bounded SP-06 profiling arm.
    pub handoff_object_store_profile: Option<HandoffObjectStoreProfile>,
}

pub fn validate(row: &FailoverEvidence) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let required = [
        ("suite", row.suite.as_str()),
        ("command", row.command.as_str()),
        ("image", row.image.as_str()),
        ("image_id", row.image_id.as_str()),
        ("source_revision", row.source_revision.as_str()),
        ("chart_revision", row.chart_revision.as_str()),
        ("postgres_image", row.postgres_image.as_str()),
        ("minio_image", row.minio_image.as_str()),
        ("old_owner_id", row.old_owner_id.as_str()),
        ("new_owner_id", row.new_owner_id.as_str()),
        ("moved_endpoint", row.moved_endpoint.as_str()),
        ("topology", row.topology.as_str()),
        ("hardware", row.hardware.as_str()),
        ("fault_schedule", row.fault_schedule.as_str()),
        ("exclusions", row.exclusions.as_str()),
    ];
    for (name, value) in required {
        if value.trim().is_empty() {
            errors.push(format!("{name} must be non-empty"));
        }
    }
    if !matches!(row.schema_version, 1 | 2) {
        errors.push("schema_version must be 1 or 2".into());
    }
    if row.evidence_id != "E2_FAILOVER" {
        errors.push("evidence_id must be E2_FAILOVER".into());
    }
    if row.evidence_tier != "release" {
        errors.push("evidence_tier must be release".into());
    }
    if row.scale != "release" {
        errors.push("scale must be release".into());
    }
    if row.backend_profile != "object_log_sqlite_projection" {
        errors.push("backend_profile must be object_log_sqlite_projection".into());
    }
    if !row.bars_met {
        errors.push("bars_met must be true".into());
    }
    if row.replicas < 3 {
        errors.push("replicas must be at least 3".into());
    }
    if row.old_owner_id == row.new_owner_id {
        errors.push("replacement owner must differ".into());
    }
    if row.new_epoch <= row.old_epoch {
        errors.push("new_epoch must be strictly greater".into());
    }
    if !row.stale_append_rejected_before_mutation {
        errors.push("stale append fence assertion missing".into());
    }
    if !row.snapshot_tail_recovered {
        errors.push("snapshot+tail recovery assertion missing".into());
    }
    if row.visible_items_after != row.visible_items_before {
        errors.push("visible state changed across recovery".into());
    }
    if row.lost_work != 0 {
        errors.push("lost_work must be zero".into());
    }
    if row.double_leases != 0 {
        errors.push("double_leases must be zero".into());
    }
    if row.corrupt_writes != 0 {
        errors.push("corrupt_writes must be zero".into());
    }
    if row.moved_count != 1 {
        errors.push("moved_count must be exactly one".into());
    }
    if row.retry_count != 1 {
        errors.push("retry_count must be exactly one".into());
    }
    if !row.retry_succeeded {
        errors.push("one-hop retry must succeed".into());
    }
    if row.duration_ms == 0 {
        errors.push("duration_ms must be positive".into());
    }
    if row.schema_version == 1 && row.handoff_object_store_profile.is_some() {
        errors.push("schema v1 cannot carry a handoff object-store profile".into());
    }
    if let Some(profile) = &row.handoff_object_store_profile {
        if profile.samples == 0 {
            errors.push("handoff profile samples must be positive".into());
        }
        if profile.queue_items == 0 {
            errors.push("handoff profile queue_items must be positive".into());
        }
        if profile.scripted_request_latency_ms == 0 {
            errors.push("handoff profile scripted latency must be positive".into());
        }
        if profile.physical_requests == 0 {
            errors.push("handoff profile physical_requests must be positive".into());
        }
        if profile.p95_modeled_handoff_latency_ms == 0 {
            errors.push("handoff profile modeled p95 must be positive".into());
        }
        let modeled_total = profile
            .physical_requests
            .saturating_mul(profile.scripted_request_latency_ms);
        if profile.p95_modeled_handoff_latency_ms > modeled_total {
            errors.push("handoff profile modeled p95 exceeds total modeled latency".into());
        }
        if profile.scripted_request_latency_ms != 0
            && profile.p95_modeled_handoff_latency_ms % profile.scripted_request_latency_ms != 0
        {
            errors.push("handoff profile modeled p95 is off the scripted latency grid".into());
        }
        if profile.scripted_request_latency_ms != 0
            && profile.p95_perfect_cache_latency_ms % profile.scripted_request_latency_ms != 0
        {
            errors
                .push("handoff profile perfect-cache p95 is off the scripted latency grid".into());
        }
        if profile.p95_perfect_cache_latency_ms > profile.p95_modeled_handoff_latency_ms {
            errors.push("handoff profile perfect-cache p95 exceeds cold p95".into());
        }
        if profile.immutable_gets > profile.physical_requests {
            errors.push("handoff profile immutable GETs exceed physical requests".into());
        }
        if profile.repeated_immutable_gets > profile.immutable_gets {
            errors.push("handoff profile repeated immutable GETs exceed immutable GETs".into());
        }
        if profile.avoidable_immutable_gets > profile.immutable_gets {
            errors.push("handoff profile avoidable immutable GETs exceed immutable GETs".into());
        }
        if profile.manifest_candidate_gets > profile.immutable_gets
            || profile.repeated_manifest_candidate_gets > profile.manifest_candidate_gets
            || profile.repeated_manifest_candidate_gets > profile.repeated_immutable_gets
            || profile.segment_gets > profile.immutable_gets
            || profile
                .manifest_candidate_gets
                .saturating_add(profile.segment_gets)
                > profile.immutable_gets
        {
            errors.push("handoff profile immutable class counts are inconsistent".into());
        }
        if (profile.immutable_gets == 0) != (profile.immutable_response_bytes == 0) {
            errors.push("handoff profile immutable bytes/read presence is inconsistent".into());
        }
        if profile.unapplied_tail_per_handoff {
            if profile.tail_commands_replayed != profile.samples
                || profile.segment_gets != profile.samples
            {
                errors.push("handoff tail arm must replay and fetch one segment per sample".into());
            }
        } else if profile.tail_commands_replayed != 0 || profile.segment_gets != 0 {
            errors.push("handoff clean arm cannot replay or fetch tail segments".into());
        }
        if profile.first_local_read_requests != 0 {
            errors.push("handoff profile first local read performed object-store requests".into());
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn verify_file(path: &Path) -> Result<FailoverEvidence, Vec<String>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| vec![format!("read {}: {error}", path.display())])?;
    let row: FailoverEvidence = serde_json::from_str(&contents)
        .map_err(|error| vec![format!("decode {}: {error}", path.display())])?;
    validate(&row)?;
    Ok(row)
}
