//! SQL shared by relational projection drivers.

/// Read the durable projection cursor for one queue.
pub const SELECT_RELATIONAL_CURSOR: &str =
    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2";

/// Read materialized item identifiers for counter restoration.
pub const SELECT_MATERIALIZED_ITEM_IDS: &str =
    "SELECT item_id FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2";

/// Minimal async projection SQL shared by embedded relational drivers.
pub mod async_projection {
    pub const SELECT_QUEUE_DEFINITION: &str =
        "SELECT definition FROM queues WHERE tenant=?1 AND queue=?2";
    pub const INSERT_QUEUE: &str =
        "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)";
    pub const INSERT_QUEUE_IF_ABSENT: &str = "INSERT INTO queues(tenant,queue,definition,paused) \
        VALUES(?1,?2,?3,0) ON CONFLICT(tenant,queue) DO NOTHING";
    pub const INSERT_CURSOR: &str = "INSERT INTO relational_cursor(tenant,queue,next_seq,\
        next_item_seq,assignment_epoch) VALUES(?1,?2,0,0,0)";
    pub const INSERT_CURSOR_IF_ABSENT: &str = "INSERT INTO relational_cursor(tenant,queue,next_seq,\
        next_item_seq,assignment_epoch) VALUES(?1,?2,0,0,0) \
        ON CONFLICT(tenant,queue) DO NOTHING";
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
    pub const INSERT_ITEM: &str = "INSERT INTO fireweed_items \
        (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
         not_before,eligible_since,group_key,cohort_size,recurrence_until,payload,fields,metadata,\
         entity_document,retry_count,item_version,lease_token_hash,lease_expires_at,worker_id,\
         last_command_sequence,created_at,updated_at,terminal_at,terminal_command_epoch,fenced,\
         superseded,max_attempts,created_seq) VALUES \
        (?1,?2,?3,?4,'Pending',?5,?6,?7,?8,?9,?10,NULL,?11,?12,?13,?14,0,1,NULL,NULL,NULL,\
         ?15,?16,?16,NULL,NULL,0,0,?17,?18)";
    pub const SELECT_ELIGIBLE: &str = "SELECT item_id FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
        AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
        JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
        AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
        AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
        ORDER BY priority_sort,created_seq LIMIT ?4";
    /// FIFO claim scan: walk rowid after a process-local hint. Same predicate as the un-hinted path
    /// minus the gate anti-join (caller uses this only when the queue has no blocked gates).
    pub const SELECT_ELIGIBLE_FIFO_ROWID: &str = "SELECT item_id FROM fireweed_items NOT INDEXED \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
        AND eligible_since IS NOT NULL AND rowid>=?5 ORDER BY rowid LIMIT ?4";
    pub const SELECT_ELIGIBLE_NO_GATES: &str = "SELECT item_id FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
        AND eligible_since IS NOT NULL ORDER BY priority_sort, created_seq LIMIT ?4";
    pub const SELECT_QUEUE_PAUSED: &str = "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2";
    pub const SELECT_HAS_BLOCKED_GATES: &str =
        "SELECT 1 FROM fireweed_gate_state WHERE tenant_id=?1 AND queue_id=?2 LIMIT 1";
    pub const SELECT_ITEM_CLAIM_FILTERABLE: &str = "SELECT item_id FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
        AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
        JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
        AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
        AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
        AND (?5 IS NULL OR group_key=?5) \
        AND NOT EXISTS (SELECT 1 FROM json_each(?6) wanted \
          WHERE NOT EXISTS (SELECT 1 FROM json_each(fireweed_items.metadata) actual \
            WHERE actual.key=wanted.key AND actual.value=wanted.value \
              AND actual.type=wanted.type)) \
        ORDER BY priority_sort,created_seq LIMIT ?4";
    pub const INSERT_ID_HIGH_WATER: &str = "INSERT INTO fireweed_id_high_water(tenant,queue,item_id) \
        VALUES(?1,?2,?3) ON CONFLICT(tenant,queue) DO UPDATE SET item_id=excluded.item_id \
        WHERE length(excluded.item_id)>length(fireweed_id_high_water.item_id) \
           OR (length(excluded.item_id)=length(fireweed_id_high_water.item_id) \
               AND excluded.item_id>fireweed_id_high_water.item_id)";
    pub const SELECT_ID_HIGH_WATER: &str =
        "SELECT item_id FROM fireweed_id_high_water WHERE tenant=?1 AND queue=?2";
    pub const EXPIRED_LEASES_BOUNDED: &str = "SELECT item_id FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' AND cohort_size IS NULL \
        AND fenced=0 AND superseded=0 AND lease_expires_at IS NOT NULL AND lease_expires_at<?3 \
        ORDER BY item_id LIMIT ?4";
    pub const SELECT_ELIGIBLE_FILTERABLE: &str = "SELECT item_id,group_key,metadata FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
        AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=?3) \
        AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
        JOIN fireweed_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
        AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=fireweed_items.tenant_id \
        AND ig.queue_id=fireweed_items.queue_id AND ig.item_id=fireweed_items.item_id) \
        ORDER BY priority_sort,created_seq LIMIT ?4 OFFSET ?5";
    pub const SELECT_ITEM_STATE: &str = "SELECT lifecycle_state FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3";
    pub const SELECT_ITEM_VERSION: &str = "SELECT item_version FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3";
    pub const SELECT_CLAIMED_ITEM: &str = "SELECT client_item_key,item_version,priority,group_key,\
        not_before,lease_expires_at,retry_count,payload,fields,metadata FROM fireweed_items \
        WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased' AND item_id=?3";
    pub const SELECT_ITEM_GATES: &str = "SELECT gate_key FROM fireweed_item_gates \
        WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 ORDER BY gate_key";
    pub const SELECT_DEFINITIONS: &str = "SELECT definition FROM queues ORDER BY tenant,queue";
    /// Point-get one opaque non-work side record by key (recovery/audit read, bead fireweed-82211ac4).
    pub const SELECT_SIDE_RECORD: &str = "SELECT payload FROM fireweed_side_records \
        WHERE tenant_id=?1 AND queue_id=?2 AND key=?3";
    /// Key-ascending prefix seek over side records: `?3` is the start key (a resume cursor or the
    /// prefix itself) and `?4` is `page_size + 1`, so the overflow row becomes the next cursor
    /// (mirrors `fireweed-sqlite`'s `side_records_by_prefix_sql`).
    pub const SELECT_SIDE_RECORDS_BY_PREFIX: &str = "SELECT key,payload FROM fireweed_side_records \
        WHERE tenant_id=?1 AND queue_id=?2 AND key>=?3 ORDER BY key ASC LIMIT ?4";
    /// Retained whole-body commit outcome for one `request_id` (recovery/explain read). Ignores the
    /// body fingerprint — the reader has only the id — mirroring `fireweed-sqlite`'s
    /// `read_commit_recovery`.
    pub const SELECT_COMMIT_RECOVERY: &str = "SELECT response_payload \
        FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
        AND operation='commit' AND request_id=?3";
    pub const SELECT_CLAIM_BY_QUERY_REPLAYS: &str = "SELECT request_id,response_payload \
        FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 \
        AND operation='claim_by_query'";
    pub const EXTEND_CLAIM_BY_QUERY_REPLAY: &str = "UPDATE fireweed_request_idempotency \
        SET expires_at=max(expires_at,?4) WHERE tenant_id=?1 AND queue_id=?2 \
        AND operation='claim_by_query' AND request_id=?3";
    pub const INSERT_ITEM_GATE: &str = "INSERT INTO fireweed_item_gates \
        (tenant_id,queue_id,item_id,gate_key) VALUES(?1,?2,?3,?4) \
        ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING";
    pub const SELECT_LIVE_FIELDS: &str = "SELECT fields FROM fireweed_items WHERE tenant_id=?1 \
        AND queue_id=?2 AND item_id=?3 AND lifecycle_state IN ('Pending','Leased') \
        AND superseded=0 AND fenced=0";
    pub const UPDATE_FIELDS_KEEP_PAYLOAD: &str = "UPDATE fireweed_items SET fields=?4,\
        item_version=item_version+1,updated_at=?5,last_command_sequence=?6 WHERE tenant_id=?1 \
        AND queue_id=?2 AND item_id=?3 AND lifecycle_state IN ('Pending','Leased') \
        AND superseded=0 AND fenced=0";
    pub const UPDATE_FIELDS_SET_PAYLOAD: &str = "UPDATE fireweed_items SET fields=?4,payload=?5,\
        item_version=item_version+1,updated_at=?6,last_command_sequence=?7 WHERE tenant_id=?1 \
        AND queue_id=?2 AND item_id=?3 AND lifecycle_state IN ('Pending','Leased') \
        AND superseded=0 AND fenced=0";
    pub const UPDATE_ENTITY_DOCUMENT: &str = "UPDATE fireweed_items SET entity_document=?4,index_fields=?5 \
        WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3";
    pub const SUPERSEDE_ITEM: &str = "UPDATE fireweed_items SET superseded=1,updated_at=?4,\
        last_command_sequence=?5 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3";
    pub const PAUSE_QUEUE: &str =
        "UPDATE queues SET paused=1,pause_drain_intake=?3 WHERE tenant=?1 AND queue=?2";
    pub const RESUME_QUEUE: &str =
        "UPDATE queues SET paused=0,pause_drain_intake=0 WHERE tenant=?1 AND queue=?2";
    pub const UPSERT_KEY_RETENTION: &str = "INSERT INTO fireweed_item_key_retention \
        (tenant_id,queue_id,client_item_key,item_id,expires_at) VALUES(?1,?2,?3,?4,?5) \
        ON CONFLICT(tenant_id,queue_id,client_item_key) DO UPDATE SET \
        item_id=excluded.item_id,expires_at=excluded.expires_at";
    pub const SET_GATE_BLOCKED: &str = "INSERT INTO fireweed_gate_state \
        (tenant_id,queue_id,gate_key) VALUES(?1,?2,?3) \
        ON CONFLICT(tenant_id,queue_id,gate_key) DO NOTHING";
    pub const SET_GATE_UNBLOCKED: &str = "DELETE FROM fireweed_gate_state \
        WHERE tenant_id=?1 AND queue_id=?2 AND gate_key=?3";

    pub fn claim_items(placeholders: usize) -> String {
        let ids = vec!["?"; placeholders].join(",");
        format!(
            "UPDATE fireweed_items SET lifecycle_state='Leased',lease_token_hash=?,\
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
            "UPDATE fireweed_items SET lease_expires_at=?,item_version=item_version+1,\
             updated_at=?,last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn reassign_lease(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE fireweed_items SET lease_token_hash=?,lease_expires_at=?,\
             retry_count=retry_count+1,item_version=item_version+1,updated_at=?,\
             last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn select_retry_info(placeholders: usize) -> String {
        with_item_ids(
            "SELECT item_id,retry_count,max_attempts FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn finalize_items(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE fireweed_items SET lifecycle_state=?,lease_token_hash=NULL,\
             lease_expires_at=NULL,worker_id=NULL,fenced=0,item_version=item_version+1,\
             retry_count=CASE WHEN ? THEN 0 ELSE retry_count END,terminal_at=?,\
             terminal_command_epoch=?,updated_at=?,last_command_sequence=? \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn finalize_backoff(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE fireweed_items SET not_before=?,eligible_since=? \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn lease_expired(placeholders: usize) -> String {
        with_item_ids(
            "UPDATE fireweed_items SET lifecycle_state='Pending',lease_token_hash=NULL,\
             lease_expires_at=NULL,worker_id=NULL,item_version=item_version+1,updated_at=?,\
             last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn fence_lease(placeholders: usize, fenced: bool) -> String {
        with_item_ids(
            if fenced {
                "UPDATE fireweed_items SET fenced=1 WHERE tenant_id=? AND queue_id=? AND item_id IN"
            } else {
                "UPDATE fireweed_items SET fenced=0 WHERE tenant_id=? AND queue_id=? AND item_id IN"
            },
            placeholders,
        )
    }

    pub fn select_purge_items(placeholders: usize) -> String {
        with_item_ids(
            "SELECT item_id,client_item_key,lifecycle_state FROM fireweed_items \
             WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn delete_items(placeholders: usize) -> String {
        with_item_ids(
            "DELETE FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn delete_item_gates(placeholders: usize) -> String {
        with_item_ids(
            "DELETE FROM fireweed_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }

    pub fn delete_item_indexes(placeholders: usize) -> String {
        with_item_ids(
            "DELETE FROM fireweed_item_index WHERE tenant_id=? AND queue_id=? AND item_id IN",
            placeholders,
        )
    }
}
