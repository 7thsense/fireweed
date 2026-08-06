//! TP-005 `million-cycle-v1` fixed-work gate: insert 1M / modify 500K / read+verify 1M.

use std::time::Instant;

use bytes::Bytes;
use fireweed::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateOutcome, BatchUpdateRequest, BatchUpdateValue,
    ClientItemKey, Fireweed, NewItem, QueueDefinition, QueueKey, RequestId,
};
use serde::{Deserialize, Serialize};

/// Fixed work sizes (TP-005).
pub const INSERT_ITEMS: u64 = 1_000_000;
pub const MODIFY_ITEMS: u64 = 500_000;
pub const BATCH: usize = 1_000;
pub const WARMUP_ITEMS: u64 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MillionCycleResult {
    pub cell: String,
    pub insert_ns: u64,
    pub modify_ns: u64,
    pub read_verify_ns: u64,
    pub insert_items: u64,
    pub modify_items: u64,
    pub read_items: u64,
    pub reopen_ok: bool,
}

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn client_key(i: u64) -> ClientItemKey {
    ClientItemKey::new(format!("mc-{i:09}")).expect("client key")
}

fn item(i: u64, payload_tag: u8) -> NewItem {
    NewItem {
        client_item_key: Some(client_key(i)),
        priority: Some(fireweed::PriorityValue::Int64(i as i64)),
        group_key: None,
        not_before: None,
        payload: Some(Bytes::from(vec![payload_tag; 16])),
        fields: Default::default(),
        metadata: Default::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity: None,
    }
}

/// Unmeasured warmup of `WARMUP_ITEMS` then the timed 1M/500K/1M cycle.
pub async fn run_million_cycle(
    fireweed: &Fireweed,
    definition: QueueDefinition,
    queue: QueueKey,
    cell: &str,
) -> Result<MillionCycleResult, String> {
    fireweed
        .create_queue(definition)
        .await
        .map_err(|e| format!("million-cycle create_queue: {e}"))?;

    // Warmup (untimed).
    let mut i = 0u64;
    while i < WARMUP_ITEMS {
        let end = (i + BATCH as u64).min(WARMUP_ITEMS);
        let batch: Vec<_> = (i..end).map(|n| item(n + 9_000_000_000, 0)).collect();
        let rid = RequestId::new(format!("mc-warm-{i}"))
            .map_err(|e| format!("warmup request id: {e}"))?;
        fireweed
            .push_batch_with_request_id(&queue, rid, batch)
            .await
            .map_err(|e| format!("warmup push: {e}"))?;
        i = end;
    }

    // Insert 1M.
    let insert_start = Instant::now();
    let mut inserted = 0u64;
    while inserted < INSERT_ITEMS {
        let end = (inserted + BATCH as u64).min(INSERT_ITEMS);
        let batch: Vec<_> = (inserted..end).map(|n| item(n, 1)).collect();
        let rid = RequestId::new(format!("mc-ins-{inserted}"))
            .map_err(|e| format!("insert request id: {e}"))?;
        let outcome = fireweed
            .push_batch_with_request_id(&queue, rid, batch)
            .await
            .map_err(|e| format!("insert push at {inserted}: {e}"))?;
        if outcome.len() != (end - inserted) as usize {
            return Err(format!(
                "insert batch size mismatch at {inserted}: got {}",
                outcome.len()
            ));
        }
        inserted = end;
    }
    let insert_ns = nanos(insert_start);

    // Modify first 500K → version 2.
    let modify_start = Instant::now();
    let mut modified = 0u64;
    while modified < MODIFY_ITEMS {
        let end = (modified + BATCH as u64).min(MODIFY_ITEMS);
        let updates: Vec<_> = (modified..end)
            .map(|n| BatchUpdateEntry {
                item_ref: BatchUpdateItemRef::ClientItemKey(client_key(n)),
                expected_item_version: Some(1),
                priority: BatchUpdateValue::Keep,
                not_before: BatchUpdateValue::Keep,
                payload: BatchUpdateValue::Replace(Some(Bytes::from(vec![2u8; 16]))),
                metadata: BatchUpdateValue::Keep,
                gate_keys: BatchUpdateValue::Keep,
                fields: BatchUpdateValue::Keep,
            })
            .collect();
        let rid = RequestId::new(format!("mc-mod-{modified}"))
            .map_err(|e| format!("modify request id: {e}"))?;
        let response = fireweed
            .batch_update(
                &queue,
                BatchUpdateRequest {
                    request_id: rid,
                    updates,
                },
            )
            .await
            .map_err(|e| format!("batch_update at {modified}: {e}"))?;
        for (idx, outcome) in response.results.iter().enumerate() {
            match outcome {
                BatchUpdateOutcome::Updated {
                    item_version: 2, ..
                } => {}
                other => {
                    return Err(format!(
                        "modify outcome at {}: expected Updated v2, got {other:?}",
                        modified + idx as u64
                    ));
                }
            }
        }
        modified = end;
    }
    let modify_ns = nanos(modify_start);

    // Read + verify all 1M.
    let read_start = Instant::now();
    let mut read = 0u64;
    while read < INSERT_ITEMS {
        let end = (read + BATCH as u64).min(INSERT_ITEMS);
        let keys: Vec<_> = (read..end).map(client_key).collect();
        let items = fireweed
            .live_items(&queue, keys)
            .await
            .map_err(|e| format!("live_items at {read}: {e}"))?;
        if items.len() != (end - read) as usize {
            return Err(format!(
                "live_items count at {read}: expected {}, got {}",
                end - read,
                items.len()
            ));
        }
        for (offset, maybe_view) in items.iter().enumerate() {
            let n = read + offset as u64;
            let view = maybe_view
                .as_ref()
                .ok_or_else(|| format!("item {n} missing from live_items"))?;
            let expected_version = if n < MODIFY_ITEMS { 2 } else { 1 };
            let expected_tag = if n < MODIFY_ITEMS { 2u8 } else { 1u8 };
            if view.item_version != expected_version {
                return Err(format!(
                    "item {n} version: expected {expected_version}, got {}",
                    view.item_version
                ));
            }
            let payload = view
                .payload
                .as_ref()
                .ok_or_else(|| format!("item {n} missing payload"))?;
            if payload.as_ref() != [expected_tag; 16].as_slice() {
                return Err(format!("item {n} payload mismatch"));
            }
        }
        read = end;
    }
    let read_verify_ns = nanos(read_start);

    Ok(MillionCycleResult {
        cell: cell.into(),
        insert_ns,
        modify_ns,
        read_verify_ns,
        insert_items: INSERT_ITEMS,
        modify_items: MODIFY_ITEMS,
        read_items: INSERT_ITEMS,
        reopen_ok: false, // filled by orchestrator after reopen
    })
}
