//! Recovery and disposable-projection maintenance workloads for TP-005.
//!
//! Timed paths use only the public `fireweed` facade. Service construction,
//! async-projection catch-up, cleanup, and evidence persistence stay with the orchestrator.

use std::{fmt::Display, time::Instant};

use fireweed::{
    ClaimCompatibility, ClaimedItem, Fireweed, ItemId, NewItem, ProjectionControlCapabilities,
    QueueDefinition, QueueKey, QueueMetrics, RequestId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Shape, make_batch};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PopulationEvidence {
    pub shape: String,
    pub items: u64,
    pub batch: usize,
    pub identity_sha256: String,
    pub content_sha256: String,
    pub metrics: QueueMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionVerificationEvidence {
    pub compatible: bool,
    pub projection_sequence: u64,
    pub authoritative_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryResult {
    pub cell: String,
    pub repetition: usize,
    pub population: PopulationEvidence,
    /// Time spent inside the synchronous facade-construction callback.
    pub reopen_ns: u64,
    /// Time from facade return through exact pending metrics and identity verification.
    pub verify_ns: u64,
    pub drain_ns: u64,
    pub reopened_metrics: QueueMetrics,
    pub drained_metrics: QueueMetrics,
    pub drained_content_sha256: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionControlCapabilitiesEvidence {
    pub verify: bool,
    pub delete: bool,
    pub rebuild: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRebuildEvidence {
    pub snapshot_used: bool,
    pub tail_commands_replayed: u64,
    pub projection_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionMaintenanceResult {
    pub cell: String,
    pub repetition: usize,
    pub population: PopulationEvidence,
    pub capabilities: ProjectionControlCapabilitiesEvidence,
    pub verify_before_ns: u64,
    pub verification_before: ProjectionVerificationEvidence,
    pub delete_ns: u64,
    pub rebuild_ns: u64,
    pub rebuild: ProjectionRebuildEvidence,
    pub verify_after_ns: u64,
    pub verification_after: ProjectionVerificationEvidence,
    pub post_rebuild_metrics: QueueMetrics,
    pub post_rebuild_identity_sha256: String,
    pub drain_ns: u64,
    pub drained_metrics: QueueMetrics,
    pub drained_content_sha256: String,
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn validate_workload(items: u64, batch: usize) -> Result<(), String> {
    if items == 0 || batch == 0 {
        return Err("lifecycle workload items and batch must be non-zero".into());
    }
    if !items.is_multiple_of(batch as u64) {
        return Err("lifecycle workload items must be exactly divisible by batch".into());
    }
    Ok(())
}

fn identity_sha256(ids: &[ItemId]) -> String {
    let mut ids = ids.iter().map(ItemId::as_u64).collect::<Vec<_>>();
    ids.sort_unstable();
    let mut digest = Sha256::new();
    for id in ids {
        digest.update(id.to_be_bytes());
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn content_record(
    priority: &Option<fireweed::PriorityValue>,
    payload: Option<&fireweed::Bytes>,
    fields: &std::collections::BTreeMap<String, fireweed::Bytes>,
) -> Vec<u8> {
    let mut record = format!("priority={priority:?};payload=").into_bytes();
    if let Some(payload) = payload {
        record.extend_from_slice(payload);
    }
    for (key, value) in fields {
        record.extend_from_slice(b";field=");
        record.extend_from_slice(key.as_bytes());
        record.push(b'=');
        record.extend_from_slice(value);
    }
    record
}

fn content_sha256_records(mut records: Vec<Vec<u8>>) -> String {
    records.sort();
    let mut digest = Sha256::new();
    for record in records {
        digest.update((record.len() as u64).to_be_bytes());
        digest.update(record);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn new_item_content(items: &[NewItem]) -> Vec<Vec<u8>> {
    items
        .iter()
        .map(|item| content_record(&item.priority, item.payload.as_ref(), &item.fields))
        .collect()
}

fn claimed_content(items: &[ClaimedItem]) -> Vec<Vec<u8>> {
    items
        .iter()
        .map(|item| content_record(&item.priority, item.payload.as_ref(), &item.fields))
        .collect()
}

async fn inspect_pending(
    fireweed: &Fireweed,
    queue: &QueueKey,
    expected_items: u64,
) -> Result<(QueueMetrics, Vec<ItemId>), String> {
    let metrics = fireweed
        .metrics(queue)
        .await
        .map_err(|error| format!("read metrics: {error}"))?;
    if metrics.pending != expected_items || metrics.leased != 0 {
        return Err(format!(
            "pending population differs: pending={} leased={} expected={expected_items}",
            metrics.pending, metrics.leased
        ));
    }
    let limit = usize::try_from(expected_items)
        .map_err(|_| "pending population does not fit usize".to_owned())?;
    let views = fireweed
        .peek(queue, limit)
        .await
        .map_err(|error| format!("peek pending population: {error}"))?;
    if views.len() != limit {
        return Err(format!(
            "peek returned {} identities, expected {limit}",
            views.len()
        ));
    }
    Ok((
        metrics,
        views.into_iter().map(|view| view.item_id).collect(),
    ))
}

async fn populate(
    fireweed: &Fireweed,
    definition: QueueDefinition,
    queue: &QueueKey,
    shape: &Shape,
    request_prefix: &str,
    items: u64,
    batch: usize,
) -> Result<(PopulationEvidence, Vec<ItemId>), String> {
    validate_workload(items, batch)?;
    fireweed
        .create_queue(definition.clone())
        .await
        .map_err(|error| format!("create lifecycle queue: {error}"))?;
    let persisted = fireweed
        .queue_definition(queue)
        .await
        .map_err(|error| format!("read lifecycle queue definition: {error}"))?;
    if persisted != definition {
        return Err(
            "persisted lifecycle queue definition differs from requested definition".into(),
        );
    }

    let mut accepted = Vec::with_capacity(items as usize);
    let mut content_records = Vec::with_capacity(items as usize);
    for offset in (0..items).step_by(batch) {
        let request_id = RequestId::new(format!("{request_prefix}-{offset}"))
            .map_err(|error| format!("lifecycle request id: {error}"))?;
        let batch_items = make_batch(shape, offset, batch);
        content_records.extend(new_item_content(&batch_items));
        let outcome = fireweed
            .push_batch_with_request_id(queue, request_id, batch_items)
            .await
            .map_err(|error| format!("populate lifecycle queue: {error}"))?;
        if outcome.len() != batch {
            return Err(format!(
                "populate returned {} ids, expected {batch}",
                outcome.len()
            ));
        }
        accepted.extend(outcome.into_item_ids());
    }
    accepted.sort_unstable();
    accepted.dedup();
    if accepted.len() != items as usize {
        return Err(format!(
            "population contains {} unique ids, expected {items}",
            accepted.len()
        ));
    }
    let (metrics, visible) = inspect_pending(fireweed, queue, items).await?;
    let expected_digest = identity_sha256(&accepted);
    if identity_sha256(&visible) != expected_digest {
        return Err("pending identities differ from accepted identities".into());
    }
    Ok((
        PopulationEvidence {
            shape: shape.name.into(),
            items,
            batch,
            identity_sha256: expected_digest,
            content_sha256: content_sha256_records(content_records),
            metrics,
        },
        accepted,
    ))
}

async fn drain_exact(
    fireweed: &Fireweed,
    queue: &QueueKey,
    expected: &[ItemId],
    batch: usize,
) -> Result<(u64, QueueMetrics, String), String> {
    let started = Instant::now();
    let mut claimed = Vec::with_capacity(expected.len());
    let mut content_records = Vec::with_capacity(expected.len());
    while claimed.len() < expected.len() {
        let response = fireweed
            .claim_response_with(queue, batch, 3_600_000, ClaimCompatibility::default())
            .await
            .map_err(|error| format!("claim lifecycle population: {error}"))?;
        if response.items.len() != batch {
            return Err(format!(
                "lifecycle claim returned {} items, expected {batch}",
                response.items.len()
            ));
        }
        let ids = response
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>();
        content_records.extend(claimed_content(&response.items));
        fireweed
            .ack(queue, ids.iter().copied())
            .await
            .map_err(|error| format!("ack lifecycle population: {error}"))?;
        claimed.extend(ids);
    }
    if identity_sha256(&claimed) != identity_sha256(expected) {
        return Err("drained identities differ from accepted identities".into());
    }
    let metrics = fireweed
        .metrics(queue)
        .await
        .map_err(|error| format!("read drained metrics: {error}"))?;
    if metrics.pending != 0 || metrics.leased != 0 {
        return Err(format!(
            "lifecycle queue not drained: pending={} leased={}",
            metrics.pending, metrics.leased
        ));
    }
    Ok((
        elapsed_ns(started),
        metrics,
        content_sha256_records(content_records),
    ))
}

/// Create and verify the durable population that an orchestrator will close and reopen.
pub async fn seed_recovery_population(
    fireweed: &Fireweed,
    definition: QueueDefinition,
    queue: &QueueKey,
    shape: &Shape,
    request_prefix: &str,
    items: u64,
    batch: usize,
) -> Result<PopulationEvidence, String> {
    populate(
        fireweed,
        definition,
        queue,
        shape,
        request_prefix,
        items,
        batch,
    )
    .await
    .map(|(evidence, _)| evidence)
}

/// Time a synchronous reopen, then prove and drain the exact seeded population.
///
/// The caller first waits for any async projection to catch up and drops the
/// original facade. The callback constructs a new facade for the same namespace.
pub async fn reopen_verify_and_drain<F, E>(
    cell: &str,
    repetition: usize,
    queue: &QueueKey,
    population: PopulationEvidence,
    reopen: F,
) -> Result<RecoveryResult, String>
where
    F: FnOnce() -> Result<Fireweed, E>,
    E: Display,
{
    validate_workload(population.items, population.batch)?;
    let reopen_started = Instant::now();
    let fireweed = reopen().map_err(|error| format!("reopen lifecycle facade: {error}"))?;
    let reopen_ns = elapsed_ns(reopen_started);

    let verify_started = Instant::now();
    let (reopened_metrics, reopened_ids) =
        inspect_pending(&fireweed, queue, population.items).await?;
    let verify_ns = elapsed_ns(verify_started);
    if identity_sha256(&reopened_ids) != population.identity_sha256 {
        return Err("reopened pending identities differ from seeded identities".into());
    }
    let (drain_ns, drained_metrics, drained_content_sha256) =
        drain_exact(&fireweed, queue, &reopened_ids, population.batch).await?;
    if drained_content_sha256 != population.content_sha256 {
        return Err(
            "reopened payload, fields, or priority differ from the seeded population".into(),
        );
    }
    Ok(RecoveryResult {
        cell: cell.into(),
        repetition,
        population,
        reopen_ns,
        verify_ns,
        drain_ns,
        reopened_metrics,
        drained_metrics,
        drained_content_sha256,
    })
}

fn capabilities_evidence(
    capabilities: ProjectionControlCapabilities,
) -> ProjectionControlCapabilitiesEvidence {
    ProjectionControlCapabilitiesEvidence {
        verify: capabilities.verify,
        delete: capabilities.delete,
        rebuild: capabilities.rebuild,
    }
}

async fn verify_projection_caught_up(
    fireweed: &Fireweed,
    context: &str,
) -> Result<fireweed::ProjectionVerification, String> {
    let deadline = Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let verification = fireweed
            .projection_control()
            .ok_or_else(|| "projection control disappeared".to_owned())?
            .verify()
            .await
            .map_err(|error| format!("{context}: {error}"))?;
        if verification.compatible
            && verification.projection_sequence == verification.authoritative_sequence
        {
            return Ok(verification);
        }
        if Instant::now() >= deadline {
            return Err(format!("{context}: projection did not catch up within 60s"));
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// Populate, delete and rebuild the disposable projection, verify exact state,
/// and drain the queue. Unsupported configurations fail before queue creation.
#[allow(clippy::too_many_arguments)]
pub async fn run_projection_maintenance(
    fireweed: &Fireweed,
    definition: QueueDefinition,
    queue: &QueueKey,
    shape: &Shape,
    cell: &str,
    repetition: usize,
    items: u64,
    batch: usize,
) -> Result<ProjectionMaintenanceResult, String> {
    validate_workload(items, batch)?;
    let capabilities = fireweed
        .projection_control()
        .ok_or_else(|| "projection maintenance is not applicable to this configuration".to_owned())?
        .capabilities();
    if !(capabilities.verify && capabilities.delete && capabilities.rebuild) {
        return Err(format!(
            "projection maintenance requires verify/delete/rebuild; got {capabilities:?}"
        ));
    }
    let (population, accepted) = populate(
        fireweed,
        definition,
        queue,
        shape,
        &format!("tp005-maint-{cell}-{repetition}"),
        items,
        batch,
    )
    .await?;

    let started = Instant::now();
    let before = verify_projection_caught_up(fireweed, "verify projection before delete").await?;
    let verify_before_ns = elapsed_ns(started);
    if !before.compatible || before.projection_sequence != before.authoritative_sequence {
        return Err("projection was not compatible and caught up before delete".into());
    }

    let started = Instant::now();
    fireweed
        .projection_control()
        .expect("capabilities checked")
        .delete()
        .await
        .map_err(|error| format!("delete projection: {error}"))?;
    let delete_ns = elapsed_ns(started);

    let started = Instant::now();
    let rebuilt = fireweed
        .projection_control()
        .expect("capabilities checked")
        .rebuild()
        .await
        .map_err(|error| format!("rebuild projection: {error}"))?;
    let rebuild_ns = elapsed_ns(started);

    let started = Instant::now();
    let after = verify_projection_caught_up(fireweed, "verify projection after rebuild").await?;
    let verify_after_ns = elapsed_ns(started);
    if !after.compatible
        || after.projection_sequence != after.authoritative_sequence
        || rebuilt.projection_sequence != after.projection_sequence
    {
        return Err("rebuilt projection did not converge to authoritative history".into());
    }

    let (post_rebuild_metrics, post_rebuild_ids) = inspect_pending(fireweed, queue, items).await?;
    let post_rebuild_identity_sha256 = identity_sha256(&post_rebuild_ids);
    if post_rebuild_identity_sha256 != population.identity_sha256 {
        return Err("post-rebuild pending identities differ from accepted identities".into());
    }
    let (drain_ns, drained_metrics, drained_content_sha256) =
        drain_exact(fireweed, queue, &accepted, batch).await?;
    if drained_content_sha256 != population.content_sha256 {
        return Err("rebuilt payload, fields, or priority differ from accepted state".into());
    }

    Ok(ProjectionMaintenanceResult {
        cell: cell.into(),
        repetition,
        population,
        capabilities: capabilities_evidence(capabilities),
        verify_before_ns,
        verification_before: ProjectionVerificationEvidence {
            compatible: before.compatible,
            projection_sequence: before.projection_sequence,
            authoritative_sequence: before.authoritative_sequence,
        },
        delete_ns,
        rebuild_ns,
        rebuild: ProjectionRebuildEvidence {
            snapshot_used: rebuilt.snapshot_used,
            tail_commands_replayed: rebuilt.tail_commands_replayed,
            projection_sequence: rebuilt.projection_sequence,
        },
        verify_after_ns,
        verification_after: ProjectionVerificationEvidence {
            compatible: after.compatible,
            projection_sequence: after.projection_sequence,
            authoritative_sequence: after.authoritative_sequence,
        },
        post_rebuild_metrics,
        post_rebuild_identity_sha256,
        drain_ns,
        drained_metrics,
        drained_content_sha256,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use fireweed::{
        ObjectLogAuthority, ObjectLogRuntimeConfig, ObjectLogStorage, ProjectionConfig,
        RecoveryPolicy, ResponseBarrier, SegmentConfig, open_memory, open_objectlog_sqlite,
    };

    use super::*;
    use crate::{SystemClock, all_shapes, bench_qdef, qkey};

    #[test]
    fn maintenance_rejects_memory_before_population() {
        let shape = all_shapes()[0];
        let queue = qkey("maintenance-na");
        let fireweed = open_memory(Arc::new(SystemClock));
        let error = futures::executor::block_on(run_projection_maintenance(
            &fireweed,
            bench_qdef("bench", "maintenance-na", &shape),
            &queue,
            &shape,
            "memory",
            0,
            128,
            64,
        ))
        .expect_err("memory has no disposable projection");
        assert!(error.contains("not applicable"));
        assert!(futures::executor::block_on(fireweed.queue_definition(&queue)).is_err());
    }

    fn local_config(label: &str) -> (PathBuf, ObjectLogRuntimeConfig) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "fireweed-tp005-lifecycle-{label}-{}-{nonce}",
            std::process::id()
        ));
        let config = ObjectLogRuntimeConfig {
            object_log: ObjectLogStorage::Local {
                root: base.join("log"),
            },
            authority: ObjectLogAuthority::NativeConditionalWrite,
            projection: ProjectionConfig::Sqlite {
                path: base.join("projection.sqlite"),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: SegmentConfig::new(256 * 1024, 20).expect("segments"),
            namespace: format!("tp005-lifecycle-{label}-{nonce}"),
            recovery: RecoveryPolicy::default(),
        };
        (base, config)
    }

    #[test]
    fn local_objectlog_recovery_reopens_exact_population() {
        let (base, config) = local_config("recovery");
        let shape = all_shapes()[0];
        let queue = qkey("recovery");
        let fireweed = open_objectlog_sqlite(config.clone(), Arc::new(SystemClock)).expect("open");
        let population = futures::executor::block_on(seed_recovery_population(
            &fireweed,
            bench_qdef("bench", "recovery", &shape),
            &queue,
            &shape,
            "tp005-recovery-test",
            128,
            64,
        ))
        .expect("seed");
        drop(fireweed);

        let result = futures::executor::block_on(reopen_verify_and_drain(
            "objectlog-local-sqlite-strict",
            0,
            &queue,
            population,
            || open_objectlog_sqlite(config, Arc::new(SystemClock)),
        ))
        .expect("recover");
        assert_eq!(result.reopened_metrics.pending, 128);
        assert_eq!(result.drained_metrics.pending, 0);
        std::fs::remove_dir_all(base).expect("cleanup");
    }

    #[test]
    fn local_objectlog_projection_maintenance_preserves_population() {
        let (base, config) = local_config("maintenance");
        let shape = all_shapes()[1];
        let queue = qkey("maintenance");
        let fireweed = open_objectlog_sqlite(config, Arc::new(SystemClock)).expect("open");
        let result = futures::executor::block_on(run_projection_maintenance(
            &fireweed,
            bench_qdef("bench", "maintenance", &shape),
            &queue,
            &shape,
            "objectlog-local-sqlite-strict",
            0,
            128,
            64,
        ))
        .expect("maintenance");
        assert_eq!(
            result.population.identity_sha256,
            result.post_rebuild_identity_sha256
        );
        assert_eq!(result.drained_metrics.pending, 0);
        drop(fireweed);
        std::fs::remove_dir_all(base).expect("cleanup");
    }
}
