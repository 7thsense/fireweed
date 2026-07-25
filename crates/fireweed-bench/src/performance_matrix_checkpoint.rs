use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::performance_matrix::RepetitionResult;
use crate::performance_matrix_evidence::validate_checkpoint_row;
use crate::performance_matrix_lifecycle::{ProjectionMaintenanceResult, RecoveryResult};

const CHECKPOINT_VERSION: &str = "fireweed-performance-checkpoint-v2";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixCheckpoint {
    pub schema_version: String,
    pub git_commit: String,
    pub tier: String,
    pub run_id: String,
    pub config_sha256: String,
    pub rows: Vec<RepetitionResult>,
    pub fragment_sha256: BTreeMap<String, String>,
    pub recovery: Vec<RecoveryResult>,
    pub maintenance: Vec<ProjectionMaintenanceResult>,
    pub lifecycle_sha256: BTreeMap<String, String>,
}

impl MatrixCheckpoint {
    pub fn new(git_commit: String, tier: String, run_id: String, config: &[u8]) -> Self {
        Self {
            schema_version: CHECKPOINT_VERSION.into(),
            git_commit,
            tier,
            run_id,
            config_sha256: digest(config),
            rows: Vec::new(),
            fragment_sha256: BTreeMap::new(),
            recovery: Vec::new(),
            maintenance: Vec::new(),
            lifecycle_sha256: BTreeMap::new(),
        }
    }

    pub fn validate_binding(
        &self,
        git_commit: &str,
        tier: &str,
        config: &[u8],
    ) -> Result<(), String> {
        if self.schema_version != CHECKPOINT_VERSION
            || self.git_commit != git_commit
            || self.tier != tier
            || self.config_sha256 != digest(config)
        {
            return Err("checkpoint does not match source, tier, or resolved configuration".into());
        }
        let mut keys = self.rows.iter().map(fragment_key).collect::<Vec<_>>();
        keys.sort();
        let before = keys.len();
        keys.dedup();
        if keys.len() != before {
            return Err("checkpoint contains duplicate fragments".into());
        }
        if self.fragment_sha256.len() != self.rows.len() {
            return Err("checkpoint fragment digest set is incomplete".into());
        }
        for row in &self.rows {
            validate_checkpoint_row(&self.tier, row)?;
            let key = fragment_key(row);
            let expected = self
                .fragment_sha256
                .get(&key)
                .ok_or_else(|| format!("checkpoint fragment {key} has no digest"))?;
            if expected != &fragment_digest(row)? {
                return Err(format!("checkpoint fragment {key} digest does not match"));
            }
        }
        if self.lifecycle_sha256.len() != self.recovery.len() + self.maintenance.len() {
            return Err("checkpoint lifecycle digest set is incomplete".into());
        }
        for fragment in self
            .recovery
            .iter()
            .cloned()
            .map(LifecycleFragment::Recovery)
            .chain(
                self.maintenance
                    .iter()
                    .cloned()
                    .map(LifecycleFragment::Maintenance),
            )
        {
            validate_lifecycle_fragment(&fragment)?;
            let key = lifecycle_key(&fragment);
            if self.lifecycle_sha256.get(&key) != Some(&lifecycle_digest(&fragment)?) {
                return Err(format!(
                    "checkpoint lifecycle fragment {key} digest does not match"
                ));
            }
        }
        Ok(())
    }

    pub fn contains(&self, cell: &str, shape: &str, repetition: usize) -> bool {
        self.rows
            .iter()
            .any(|row| row.cell == cell && row.shape == shape && row.repetition == repetition)
    }

    pub fn append(&mut self, row: RepetitionResult) -> Result<(), String> {
        if self.contains(&row.cell, &row.shape, row.repetition) {
            return Err("refusing duplicate checkpoint fragment".into());
        }
        let key = fragment_key(&row);
        let row_digest = fragment_digest(&row)?;
        self.rows.push(row);
        self.fragment_sha256.insert(key, row_digest);
        Ok(())
    }

    pub fn contains_recovery(&self, cell: &str, shape: &str, repetition: usize) -> bool {
        self.recovery.iter().any(|result| {
            result.cell == cell
                && result.population.shape == shape
                && result.repetition == repetition
        })
    }

    pub fn contains_maintenance(&self, cell: &str, repetition: usize) -> bool {
        self.maintenance
            .iter()
            .any(|result| result.cell == cell && result.repetition == repetition)
    }

    pub fn append_lifecycle(&mut self, fragment: LifecycleFragment) -> Result<(), String> {
        let key = lifecycle_key(&fragment);
        if self.lifecycle_sha256.contains_key(&key) {
            return Err("refusing duplicate lifecycle checkpoint fragment".into());
        }
        let digest = lifecycle_digest(&fragment)?;
        match fragment {
            LifecycleFragment::Recovery(value) => self.recovery.push(value),
            LifecycleFragment::Maintenance(value) => self.maintenance.push(value),
        }
        self.lifecycle_sha256.insert(key, digest);
        Ok(())
    }
}

fn validate_lifecycle_fragment(fragment: &LifecycleFragment) -> Result<(), String> {
    const RECOVERY_CELLS: &[&str] = &[
        "sqlite-log",
        "sqlite-relational",
        "postgres-log",
        "postgres-relational",
        "objectlog-local-direct",
        "objectlog-local-sqlite-strict",
        "objectlog-local-sqlite-async",
        "objectlog-local-postgres-strict",
        "objectlog-s3-sqlite-strict",
        "objectlog-s3-sqlite-async",
        "objectlog-s3-postgres-strict",
    ];
    const MAINTENANCE_CELLS: &[&str] = &[
        "objectlog-local-sqlite-strict",
        "objectlog-local-sqlite-async",
        "objectlog-local-postgres-strict",
        "objectlog-s3-sqlite-strict",
        "objectlog-s3-sqlite-async",
        "objectlog-s3-postgres-strict",
    ];
    match fragment {
        LifecycleFragment::Recovery(value) => {
            if !RECOVERY_CELLS.contains(&value.cell.as_str())
                || !matches!(value.population.shape.as_str(), "minimal" | "record-1k")
                || value.repetition >= 3
                || value.population.items != 12_800
                || value.population.batch != 128
                || value.population.identity_sha256.len() != 64
                || value.population.content_sha256.len() != 64
                || value.population.metrics.pending != 12_800
                || value.reopened_metrics.pending != 12_800
                || value.drained_metrics.pending != 0
                || value.drained_content_sha256 != value.population.content_sha256
            {
                return Err("checkpoint recovery fragment violates lifecycle invariants".into());
            }
        }
        LifecycleFragment::Maintenance(value) => {
            if !MAINTENANCE_CELLS.contains(&value.cell.as_str())
                || value.population.shape != "record-1k"
                || value.repetition >= 3
                || value.population.items != 12_800
                || value.population.batch != 128
                || value.population.metrics.pending != 12_800
                || value.post_rebuild_metrics.pending != 12_800
                || value.drained_metrics.pending != 0
                || value.population.identity_sha256 != value.post_rebuild_identity_sha256
                || value.population.content_sha256 != value.drained_content_sha256
            {
                return Err("checkpoint maintenance fragment violates lifecycle invariants".into());
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", content = "result", rename_all = "snake_case")]
pub enum LifecycleFragment {
    Recovery(RecoveryResult),
    Maintenance(ProjectionMaintenanceResult),
}

fn lifecycle_key(fragment: &LifecycleFragment) -> String {
    match fragment {
        LifecycleFragment::Recovery(value) => format!(
            "recovery/{}/{}/r{}",
            value.cell, value.population.shape, value.repetition
        ),
        LifecycleFragment::Maintenance(value) => {
            format!("maintenance/{}/r{}", value.cell, value.repetition)
        }
    }
}

fn lifecycle_digest(fragment: &LifecycleFragment) -> Result<String, String> {
    serde_json::to_vec(fragment)
        .map(|bytes| digest(&bytes))
        .map_err(|error| error.to_string())
}

fn fragment_key(row: &RepetitionResult) -> String {
    format!("{}/{}/r{}", row.cell, row.shape, row.repetition)
}

fn fragment_digest(row: &RepetitionResult) -> Result<String, String> {
    serde_json::to_vec(row)
        .map(|bytes| digest(&bytes))
        .map_err(|error| error.to_string())
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn write_checkpoint(path: &Path, checkpoint: &MatrixCheckpoint) -> Result<(), String> {
    let mut bytes = serde_json::to_vec_pretty(checkpoint).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = path.with_extension("checkpoint.tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(temporary, path).map_err(|error| error.to_string())
}

pub fn read_checkpoint(path: &Path) -> Result<MatrixCheckpoint, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

pub fn write_fragment(path: &Path, row: &RepetitionResult) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(row).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = path.with_extension("fragment.tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(temporary, path).map_err(|error| error.to_string())
}

pub fn read_fragment(path: &Path) -> Result<RepetitionResult, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

pub fn write_lifecycle_fragment(path: &Path, fragment: &LifecycleFragment) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(fragment).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = path.with_extension("lifecycle.tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(temporary, path).map_err(|error| error.to_string())
}

pub fn read_lifecycle_fragment(path: &Path) -> Result<LifecycleFragment, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_rejects_changed_configuration() {
        let checkpoint = MatrixCheckpoint::new("a".repeat(40), "full".into(), "run".into(), b"a");
        assert!(
            checkpoint
                .validate_binding(&"a".repeat(40), "full", b"b")
                .is_err()
        );
    }

    #[test]
    fn binding_rejects_modified_fragment() {
        let mut checkpoint =
            MatrixCheckpoint::new("a".repeat(40), "smoke".into(), "run".into(), b"config");
        let mut row = sample_row();
        checkpoint.append(row.clone()).expect("append");
        checkpoint
            .validate_binding(&"a".repeat(40), "smoke", b"config")
            .expect("valid checkpoint");
        row.append.durations_ns[0] += 1;
        checkpoint.rows[0] = row;
        assert!(
            checkpoint
                .validate_binding(&"a".repeat(40), "smoke", b"config")
                .unwrap_err()
                .contains("digest does not match")
        );
    }

    fn sample_row() -> RepetitionResult {
        use crate::performance_matrix::OperationSamples;

        let operation = |name: &str| OperationSamples {
            operation: name.into(),
            durations_ns: vec![1; 8],
            total_ns: 8,
            items: 512,
        };
        RepetitionResult {
            cell: "memory".into(),
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
        }
    }
}
