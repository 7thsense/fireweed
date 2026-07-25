//! Public-facade-only timed workload for TP-005.

use std::time::Instant;

use fireweed::{ClaimCompatibility, Fireweed, QueueDefinition, QueueKey, RequestId};
use fireweed::{
    ClaimRef, CommitEntry, CommitRequest, EngineError, EntryOutcome, FinalizeKind, LeaseToken,
    PriorityValue,
};
use serde::{Deserialize, Serialize};

use crate::{Shape, make_batch};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationSamples {
    pub operation: String,
    pub durations_ns: Vec<u64>,
    pub total_ns: u64,
    pub items: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepetitionResult {
    pub cell: String,
    pub shape: String,
    pub repetition: usize,
    pub items: u64,
    pub batch: usize,
    pub append: OperationSamples,
    pub claim: OperationSamples,
    pub finalize: OperationSamples,
    pub accepted: u64,
    pub claimed: u64,
    pub finalized: u64,
    pub projection_catchup: Option<ProjectionCatchupEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionCatchupEvidence {
    pub duration_ns: u64,
    pub poll_count: u64,
    pub compatible: bool,
    pub projection_sequence: u64,
    pub authoritative_sequence: u64,
}

pub struct RepetitionSpec<'a> {
    pub cell: &'a str,
    pub repetition: usize,
    pub items: u64,
    pub batch: usize,
}

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

pub async fn run_preflight(
    fireweed: &Fireweed,
    definition: QueueDefinition,
    queue: QueueKey,
    shape: &Shape,
    request_label: &str,
) -> Result<(), String> {
    fireweed
        .create_queue(definition)
        .await
        .map_err(|error| format!("preflight create queue: {error}"))?;
    let request_id = RequestId::new(format!("tp005-preflight-{request_label}"))
        .map_err(|error| format!("preflight request id: {error}"))?;
    let body = make_batch(shape, 0, 1);
    let first = fireweed
        .push_batch_with_request_id(&queue, request_id.clone(), body.clone())
        .await
        .map_err(|error| format!("preflight append: {error}"))?;
    let replay = fireweed
        .push_batch_with_request_id(&queue, request_id.clone(), body)
        .await
        .map_err(|error| format!("preflight replay: {error}"))?;
    if first != replay || first.len() != 1 {
        return Err("preflight request replay changed item identity".into());
    }
    match fireweed
        .push_batch_with_request_id(&queue, request_id, make_batch(shape, 1, 1))
        .await
    {
        Err(EngineError::RequestIdConflict) => {}
        Err(error) => return Err(format!("preflight conflict returned wrong error: {error}")),
        Ok(_) => return Err("preflight conflicting request id was accepted".into()),
    }
    let claimed = fireweed
        .claim_response_with(&queue, 1, 3_600_000, ClaimCompatibility::default())
        .await
        .map_err(|error| format!("preflight claim: {error}"))?;
    let item = claimed
        .items
        .first()
        .ok_or_else(|| "preflight claim returned no item".to_owned())?;
    if fireweed
        .commit_capabilities(&queue)
        .map_err(|error| format!("preflight commit capabilities: {error}"))?
        .lease_validation
    {
        let stale = ClaimRef {
            item_id: item.item_id,
            lease_token: LeaseToken::new("tp005-fabricated-stale-token")
                .map_err(|error| format!("preflight stale token: {error}"))?,
            lease_expires_at: item.lease_expires_at,
            item_version: item.item_version,
        };
        let outcomes = fireweed
            .commit(
                &queue,
                CommitRequest {
                    request_id: None,
                    entries: vec![CommitEntry {
                        claim_ref: stale,
                        finalize: FinalizeKind::Complete,
                        side_records: Vec::new(),
                        lifecycle_items: Vec::new(),
                        instance_fence: None,
                    }],
                },
            )
            .await
            .map_err(|error| format!("preflight stale commit: {error}"))?;
        if !matches!(
            outcomes.as_slice(),
            [EntryOutcome::Rejected(EngineError::StaleLease)]
        ) {
            return Err("preflight fabricated lease was not rejected as stale".into());
        }
    }
    fireweed
        .ack(&queue, vec![item.item_id])
        .await
        .map_err(|error| format!("preflight cleanup ack: {error}"))?;
    let metrics = fireweed
        .metrics(&queue)
        .await
        .map_err(|error| format!("preflight metrics: {error}"))?;
    if metrics.pending != 0 || metrics.leased != 0 {
        return Err("preflight queue did not drain".into());
    }
    Ok(())
}

pub async fn run_repetition(
    fireweed: &Fireweed,
    definition: QueueDefinition,
    queue: QueueKey,
    shape: &Shape,
    spec: RepetitionSpec<'_>,
) -> Result<RepetitionResult, String> {
    let RepetitionSpec {
        cell,
        repetition,
        items,
        batch,
    } = spec;
    fireweed
        .create_queue(definition.clone())
        .await
        .map_err(|error| format!("create queue: {error}"))?;
    let persisted = fireweed
        .queue_definition(&queue)
        .await
        .map_err(|error| format!("read queue definition: {error}"))?;
    if persisted != definition {
        return Err("persisted queue definition differs from requested definition".into());
    }

    let mut append_lat = Vec::with_capacity(items as usize / batch);
    let mut accepted_ids = Vec::with_capacity(items as usize);
    let mut expected_claim_order = Vec::with_capacity(items as usize);
    let append_total = Instant::now();
    for offset in (0..items).step_by(batch) {
        let request_id = RequestId::new(format!("tp005-{cell}-{repetition}-{offset}"))
            .map_err(|error| format!("request id: {error}"))?;
        let batch_items = make_batch(shape, offset, batch);
        let ordering = batch_items
            .iter()
            .enumerate()
            .map(|(index, item)| match item.priority {
                Some(PriorityValue::Int64(value)) => Ok((value, offset + index as u64)),
                _ => Err("matrix shape did not produce an Int64 priority".to_owned()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let started = Instant::now();
        let ids = fireweed
            .push_batch_with_request_id(&queue, request_id, batch_items)
            .await
            .map_err(|error| format!("append: {error}"))?;
        append_lat.push(nanos(started));
        if ids.len() != batch {
            return Err(format!(
                "append returned {} ids, expected {batch}",
                ids.len()
            ));
        }
        expected_claim_order.extend(
            ids.iter()
                .copied()
                .zip(ordering)
                .map(|(id, (priority, ordinal))| (id, priority, ordinal)),
        );
        accepted_ids.extend(ids);
    }
    let append_total_ns = nanos(append_total);

    let mut claim_lat = Vec::with_capacity(items as usize / batch);
    let mut finalize_lat = Vec::with_capacity(items as usize / batch);
    let mut claimed_ids = Vec::with_capacity(items as usize);
    while claimed_ids.len() < items as usize {
        let started = Instant::now();
        let response = fireweed
            .claim_response_with(&queue, batch, 3_600_000, ClaimCompatibility::default())
            .await
            .map_err(|error| format!("claim: {error}"))?;
        claim_lat.push(nanos(started));
        if response.items.len() != batch {
            return Err(format!(
                "claim returned {} items before exhaustion, expected {batch}",
                response.items.len()
            ));
        }
        if response.items.iter().any(|item| item.lease_token.is_none()) {
            return Err("claim returned an item without a lease token".into());
        }
        let ids = response
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>();
        claimed_ids.extend(ids.iter().copied());
        let started = Instant::now();
        fireweed
            .ack(&queue, ids)
            .await
            .map_err(|error| format!("finalize: {error}"))?;
        finalize_lat.push(nanos(started));
    }
    let claim_total_ns = claim_lat.iter().copied().sum();

    let claimed_in_order = claimed_ids.clone();
    expected_claim_order.sort_by_key(|(_, priority, ordinal)| (*priority, *ordinal));
    if claimed_in_order
        != expected_claim_order
            .iter()
            .map(|(id, _, _)| *id)
            .collect::<Vec<_>>()
    {
        return Err("claim order differs from priority then created-sequence order".into());
    }
    accepted_ids.sort_unstable();
    claimed_ids.sort_unstable();
    accepted_ids.dedup();
    claimed_ids.dedup();
    if accepted_ids != claimed_ids || accepted_ids.len() != items as usize {
        return Err("accepted and claimed item identities do not reconcile".into());
    }
    let metrics = fireweed
        .metrics(&queue)
        .await
        .map_err(|error| format!("metrics: {error}"))?;
    if metrics.pending != 0 || metrics.leased != 0 {
        return Err(format!(
            "queue not drained: pending={} leased={}",
            metrics.pending, metrics.leased
        ));
    }

    let operation = |name: &str, durations_ns: Vec<u64>, total_ns: u64| OperationSamples {
        operation: name.into(),
        durations_ns,
        total_ns,
        items,
    };
    Ok(RepetitionResult {
        cell: cell.into(),
        shape: shape.name.into(),
        repetition,
        items,
        batch,
        append: operation("append", append_lat, append_total_ns),
        claim: operation("claim", claim_lat, claim_total_ns),
        finalize: operation(
            "finalize",
            finalize_lat.clone(),
            finalize_lat.iter().copied().sum(),
        ),
        accepted: items,
        claimed: items,
        finalized: items,
        projection_catchup: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fireweed::open_memory;

    use super::*;
    use crate::{SystemClock, all_shapes, bench_qdef, qkey};

    #[test]
    fn smoke_repetition_reconciles_and_retains_samples() {
        let shape = all_shapes()[0];
        let queue = qkey("matrix-unit");
        let result = futures::executor::block_on(run_repetition(
            &open_memory(Arc::new(SystemClock)),
            bench_qdef("bench", "matrix-unit", &shape),
            queue,
            &shape,
            RepetitionSpec {
                cell: "memory",
                repetition: 0,
                items: 128,
                batch: 64,
            },
        ))
        .expect("run");
        assert_eq!(result.append.durations_ns.len(), 2);
        assert_eq!(result.claim.durations_ns.len(), 2);
        assert_eq!(result.finalized, 128);
    }
}
