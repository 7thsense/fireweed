//! SQL shared by relational projection drivers.

/// Read the durable projection cursor for one queue.
pub const SELECT_RELATIONAL_CURSOR: &str =
    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2";

/// Read materialized item identifiers for counter restoration.
pub const SELECT_MATERIALIZED_ITEM_IDS: &str =
    "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2";

/// Minimal async projection SQL shared by embedded relational drivers.
pub mod async_projection {
    pub const SELECT_QUEUE_DEFINITION: &str =
        "SELECT definition FROM queues WHERE tenant=?1 AND queue=?2";
    pub const INSERT_QUEUE: &str =
        "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)";
    pub const INSERT_CURSOR: &str = "INSERT INTO relational_cursor(tenant,queue,next_seq,\
        next_item_seq,assignment_epoch) VALUES(?1,?2,0,0,0)";
    pub const SELECT_CURSOR: &str = "SELECT next_seq,assignment_epoch FROM relational_cursor \
        WHERE tenant=?1 AND queue=?2";
    pub const SELECT_CURSOR_STATE: &str = "SELECT next_seq,next_item_seq,assignment_epoch FROM relational_cursor \
         WHERE tenant=?1 AND queue=?2";
    pub const SELECT_NEXT_ITEM_SEQUENCE: &str = "SELECT next_item_seq FROM relational_cursor \
        WHERE tenant=?1 AND queue=?2";
    pub const UPDATE_NEXT_ITEM_SEQUENCE: &str = "UPDATE relational_cursor SET next_item_seq=?3 \
        WHERE tenant=?1 AND queue=?2";
    pub const UPDATE_CURSOR: &str = "UPDATE relational_cursor SET next_seq=?3, \
        assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
        WHERE tenant=?1 AND queue=?2";
    pub const INSERT_ITEM: &str = "INSERT INTO pqueue_items \
        (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
         not_before,eligible_since,group_key,cohort_size,recurrence_until,payload,fields,metadata,\
         entity_document,retry_count,item_version,lease_token_hash,lease_expires_at,worker_id,\
         last_command_sequence,created_at,updated_at,terminal_at,terminal_command_epoch,fenced,\
         superseded,max_attempts,created_seq) VALUES \
        (?1,?2,?3,?4,'Pending',?5,?6,?7,?8,?9,?10,NULL,?11,?12,?13,?14,0,1,NULL,NULL,NULL,\
         ?15,?16,?16,NULL,NULL,0,0,?17,?18)";
    pub const SELECT_ELIGIBLE: &str = "SELECT item_id FROM pqueue_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
        AND eligible_since IS NOT NULL ORDER BY priority_sort,created_seq LIMIT ?4";
    pub const SELECT_ITEM_STATE: &str = "SELECT lifecycle_state FROM pqueue_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3";
    pub const SELECT_ITEM_VERSION: &str = "SELECT item_version FROM pqueue_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3";
    pub const SELECT_CLAIMED_ITEM: &str = "SELECT client_item_key,item_version,priority,group_key,\
        not_before,lease_expires_at,retry_count,payload,fields,metadata FROM pqueue_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' AND item_id=?3";
    pub const SELECT_DEFINITIONS: &str = "SELECT definition FROM queues ORDER BY tenant,queue";
    pub const SELECT_CLAIM_BY_QUERY_REPLAYS: &str = "SELECT request_id,response_payload \
        FROM pqueue_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
        AND operation='claim_by_query'";
    pub const EXTEND_CLAIM_BY_QUERY_REPLAY: &str = "UPDATE pqueue_request_idempotency \
        SET expires_at=max(expires_at,?4) WHERE tenant_id=?1 AND queue_id=?2 \
        AND operation='claim_by_query' AND request_id=?3";

    pub fn claim_items(placeholders: usize) -> String {
        let ids = vec!["?"; placeholders].join(",");
        format!(
            "UPDATE pqueue_items SET lifecycle_state='Leased',lease_token_hash=?,\
             lease_expires_at=?,worker_id=?,retry_count=retry_count+1,item_version=item_version+1,\
             updated_at=?,last_command_sequence=? WHERE tenant_id=? AND queue_id=? \
             AND lifecycle_state='Pending' AND item_id IN ({ids})"
        )
    }

    fn with_item_ids(prefix: &str, placeholders: usize) -> String {
        let ids = vec!["?"; placeholders].join(",");
        format!("{prefix} ({ids})")
    }

    pub fn renew_lease(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE pqueue_items SET lease_expires_at=?,item_version=item_version+1,\
             updated_at=?,last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn reassign_lease(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE pqueue_items SET lease_token_hash=?,lease_expires_at=?,\
             retry_count=retry_count+1,item_version=item_version+1,updated_at=?,\
             last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn select_retry_info(placeholders: usize) -> String {
        with_item_ids(
            "SELECT item_id,retry_count,max_attempts FROM pqueue_items \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn finalize_items(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE pqueue_items SET lifecycle_state=?,lease_token_hash=NULL,\
             lease_expires_at=NULL,worker_id=NULL,fenced=0,item_version=item_version+1,\
             retry_count=CASE WHEN ? THEN 0 ELSE retry_count END,terminal_at=?,\
             terminal_command_epoch=?,updated_at=?,last_command_sequence=? \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn finalize_backoff(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE pqueue_items SET not_before=?,eligible_since=? \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn lease_expired(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE pqueue_items SET lifecycle_state='Pending',lease_token_hash=NULL,\
             lease_expires_at=NULL,worker_id=NULL,item_version=item_version+1,updated_at=?,\
             last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn fence_lease(placeholders: usize, fenced: bool) -> String {
        with_item_ids(
            if fenced {
                "UPDATE pqueue_items SET fenced=1 WHERE tenant_id=? AND queue_id=? AND item_id IN"
            } else {
                "UPDATE pqueue_items SET fenced=0 WHERE tenant_id=? AND queue_id=? AND item_id IN"
            },
            placeholders,
        )
    }
}
