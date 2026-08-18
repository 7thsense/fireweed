//! Class S claim: SELECT due rows + payloads, UPDATE leased, INSERT outbox — one RelTx.

use fireweed_core::LeaseToken;
use fireweed_engine::{EngineError, EngineResult};

use crate::{RelTx, RelValue, lease_hash, rel_exec, rel_query};

/// Next due pending items, including payloads. Same eligibility predicates as
/// [`crate::sql::async_projection::SELECT_ELIGIBLE`], plus the pending-order key.
pub const SELECT_CLASS_S_DUE: &str = "SELECT item_id, client_item_key, payload, item_version, \
     retry_count FROM fireweed_items \
     WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
     AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
     AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
     JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
     AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
     AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
     ORDER BY priority_sort, created_seq, item_id LIMIT ?4";

pub const INSERT_CLAIM_OUTBOX: &str = "INSERT INTO fireweed_claim_outbox (\
     tenant_id, queue_id, outbox_id, item_ids, lease_token, lease_expires_at, \
     request_id, request_fingerprint, worker_id, claim_unit, cohort_id, created_at) \
     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)";

pub const DELETE_CLAIM_OUTBOX: &str = "DELETE FROM fireweed_claim_outbox \
     WHERE tenant_id=?1 AND queue_id=?2 AND outbox_id=?3";

pub const SELECT_CLAIM_OUTBOX_FOR_QUEUE: &str = "SELECT outbox_id, item_ids, lease_token, \
     lease_expires_at, request_id, request_fingerprint, worker_id, claim_unit, cohort_id, created_at \
     FROM fireweed_claim_outbox WHERE tenant_id=?1 AND queue_id=?2 ORDER BY created_at, outbox_id";

/// Inputs for one Class S claim transaction. The caller supplies the token and
/// outbox id; this helper does not mint them.
pub struct ClassSClaimRequest<'a> {
    pub tenant_id: &'a str,
    pub queue_id: &'a str,
    pub now_nanos: i64,
    pub limit: i64,
    pub lease_token: &'a LeaseToken,
    pub lease_expires_at: i64,
    pub outbox_id: &'a str,
    pub request_id: Option<&'a str>,
    pub request_fingerprint: Option<i64>,
    pub worker_id: Option<&'a str>,
    pub claim_unit: &'a str,
    pub cohort_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassSClaimedItem {
    pub item_id: String,
    pub client_item_key: String,
    pub payload: Option<Vec<u8>>,
    pub item_version: i64,
    pub retry_count: i64,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassSClaimResult {
    pub items: Vec<ClassSClaimedItem>,
    pub outbox_id: String,
}

/// Choose due pending rows, lease them, and persist the Claim envelope in
/// `fireweed_claim_outbox`. Empty result writes nothing.
pub fn class_s_claim(
    tx: &impl RelTx,
    request: &ClassSClaimRequest<'_>,
) -> EngineResult<ClassSClaimResult> {
    let selected = rel_query(
        tx,
        SELECT_CLASS_S_DUE,
        [
            RelValue::Text(request.tenant_id.to_string()),
            RelValue::Text(request.queue_id.to_string()),
            RelValue::Integer(request.now_nanos),
            RelValue::Integer(request.limit),
        ],
    )?;
    if selected.is_empty() {
        return Ok(ClassSClaimResult {
            items: Vec::new(),
            outbox_id: request.outbox_id.to_string(),
        });
    }

    let mut items = Vec::with_capacity(selected.len());
    let mut ids = Vec::with_capacity(selected.len());
    for row in selected {
        let item_id: String = row.get(0)?;
        ids.push(item_id.clone());
        items.push(ClassSClaimedItem {
            item_id,
            client_item_key: row.get(1)?,
            payload: row.get(2)?,
            item_version: row.get::<i64>(3)? + 1,
            retry_count: row.get::<i64>(4)? + 1,
            lease_expires_at: request.lease_expires_at,
        });
    }

    let placeholders = vec!["?"; ids.len()].join(",");
    let update_sql = format!(
        "UPDATE fireweed_items SET lifecycle_state='Leased', lease_token_hash=?, \
         lease_expires_at=?, worker_id=?, retry_count=retry_count+1, \
         item_version=item_version+1, updated_at=? \
         WHERE tenant_id=? AND queue_id=? AND lifecycle_state='Pending' AND superseded=0 \
         AND item_id IN ({placeholders})"
    );
    let mut params = vec![
        RelValue::Blob(lease_hash(request.lease_token)),
        RelValue::Integer(request.lease_expires_at),
        RelValue::from(request.worker_id),
        RelValue::Integer(request.now_nanos),
        RelValue::Text(request.tenant_id.to_string()),
        RelValue::Text(request.queue_id.to_string()),
    ];
    params.extend(ids.iter().cloned().map(RelValue::Text));
    let changed = rel_exec(tx, &update_sql, params)?;
    if changed != ids.len() {
        return Err(EngineError::Storage(format!(
            "class S lease updated {changed} rows, expected {}",
            ids.len()
        )));
    }

    let item_ids_json = serde_json::to_string(&ids).map_err(|e| EngineError::Storage(e.to_string()))?;
    rel_exec(
        tx,
        INSERT_CLAIM_OUTBOX,
        [
            RelValue::Text(request.tenant_id.to_string()),
            RelValue::Text(request.queue_id.to_string()),
            RelValue::Text(request.outbox_id.to_string()),
            RelValue::Text(item_ids_json),
            RelValue::Text(request.lease_token.as_str().to_string()),
            RelValue::Integer(request.lease_expires_at),
            RelValue::from(request.request_id),
            RelValue::from(request.request_fingerprint),
            RelValue::from(request.worker_id),
            RelValue::Text(request.claim_unit.to_string()),
            RelValue::from(request.cohort_id),
            RelValue::Integer(request.now_nanos),
        ],
    )?;

    Ok(ClassSClaimResult {
        items,
        outbox_id: request.outbox_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use fireweed_core::LeaseToken;

    use super::*;
    use crate::RelRow;

    #[derive(Clone)]
    struct Item {
        item_id: String,
        client_item_key: String,
        payload: Vec<u8>,
        item_version: i64,
        retry_count: i64,
        created_seq: i64,
        leased: bool,
    }

    struct MemTx {
        items: RefCell<Vec<Item>>,
        outbox: RefCell<Vec<String>>,
    }

    impl MemTx {
        fn with_pending(n: i64) -> Self {
            let items = (1..=n)
                .map(|seq| Item {
                    item_id: format!("item-{seq}"),
                    client_item_key: format!("key-{seq}"),
                    payload: vec![0xCA, 0xFE],
                    item_version: 1,
                    retry_count: 0,
                    created_seq: seq,
                    leased: false,
                })
                .collect();
            Self {
                items: RefCell::new(items),
                outbox: RefCell::new(Vec::new()),
            }
        }
    }

    impl RelTx for MemTx {
        fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
            if sql.starts_with("UPDATE fireweed_items SET lifecycle_state='Leased'") {
                let ids: Vec<String> = params
                    .iter()
                    .skip(6)
                    .filter_map(|value| match value {
                        RelValue::Text(text) => Some(text.clone()),
                        _ => None,
                    })
                    .collect();
                let mut items = self.items.borrow_mut();
                let mut changed = 0;
                for item in items.iter_mut() {
                    if ids.contains(&item.item_id) && !item.leased {
                        item.leased = true;
                        item.item_version += 1;
                        item.retry_count += 1;
                        changed += 1;
                    }
                }
                return Ok(changed);
            }
            if sql == INSERT_CLAIM_OUTBOX {
                let outbox_id = match &params[2] {
                    RelValue::Text(text) => text.clone(),
                    _ => return Err(EngineError::Storage("outbox id".into())),
                };
                self.outbox.borrow_mut().push(outbox_id);
                return Ok(1);
            }
            Err(EngineError::Storage(format!("unexpected execute: {sql}")))
        }

        fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>> {
            if sql != SELECT_CLASS_S_DUE {
                return Err(EngineError::Storage(format!("unexpected query: {sql}")));
            }
            let limit = match params.get(3) {
                Some(RelValue::Integer(limit)) => *limit,
                _ => 0,
            };
            let mut pending: Vec<Item> = self
                .items
                .borrow()
                .iter()
                .filter(|item| !item.leased)
                .cloned()
                .collect();
            pending.sort_by_key(|item| item.created_seq);
            Ok(pending
                .into_iter()
                .take(limit as usize)
                .map(|item| {
                    RelRow(vec![
                        RelValue::Text(item.item_id),
                        RelValue::Text(item.client_item_key),
                        RelValue::Blob(item.payload),
                        RelValue::Integer(item.item_version),
                        RelValue::Integer(item.retry_count),
                    ])
                })
                .collect())
        }
    }

    fn claim_req<'a>(
        token: &'a LeaseToken,
        outbox_id: &'a str,
        limit: i64,
    ) -> ClassSClaimRequest<'a> {
        ClassSClaimRequest {
            tenant_id: "t",
            queue_id: "q",
            now_nanos: 10,
            limit,
            lease_token: token,
            lease_expires_at: 1_000,
            outbox_id,
            request_id: None,
            request_fingerprint: None,
            worker_id: Some("worker-a"),
            claim_unit: "item",
            cohort_id: None,
        }
    }

    #[test]
    fn class_s_sequential_claims_are_disjoint_and_write_outbox() {
        let tx = MemTx::with_pending(5);
        let token_a = LeaseToken::new("token-a").expect("token");
        let token_b = LeaseToken::new("token-b").expect("token");

        let first = class_s_claim(&tx, &claim_req(&token_a, "outbox-1", 2)).expect("first claim");
        let second = class_s_claim(&tx, &claim_req(&token_b, "outbox-2", 2)).expect("second claim");

        let first_ids: Vec<&str> = first.items.iter().map(|i| i.item_id.as_str()).collect();
        let second_ids: Vec<&str> = second.items.iter().map(|i| i.item_id.as_str()).collect();
        assert_eq!(first_ids, ["item-1", "item-2"]);
        assert_eq!(second_ids, ["item-3", "item-4"]);
        assert!(
            first_ids.iter().all(|id| !second_ids.contains(id)),
            "sequential Class S claims must be disjoint"
        );
        assert_eq!(first.items[0].payload.as_deref(), Some(&[0xCA, 0xFE][..]));
        assert_eq!(first.items[0].item_version, 2);
        assert_eq!(first.items[0].retry_count, 1);
        assert_eq!(tx.outbox.borrow().as_slice(), ["outbox-1", "outbox-2"]);
        let leased = tx.items.borrow().iter().filter(|item| item.leased).count();
        let pending = tx.items.borrow().iter().filter(|item| !item.leased).count();
        assert_eq!((leased, pending), (4, 1));
    }
}
